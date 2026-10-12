use chrono::prelude::*;
use futures_util::io::{AsyncRead, AsyncReadExt};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

use uuid::Uuid;

use crate::header::WarcHeader;
use crate::record_type::RecordType;
use crate::truncated_type::TruncatedType;
use crate::Error as WarcError;

/// Content-Length comes from untrusted input, so it only seeds the buffer up to this size.
const MAX_BODY_PREALLOCATION: u64 = 16 * 1_048_576;

use streaming_trait::BodyKind;
pub use streaming_trait::{BufferedBody, EmptyBody, StreamingBody};

mod streaming_trait {
    use futures_util::io::AsyncRead;
    use std::pin::Pin;
    use std::task::{ready, Context, Poll};

    /// An associated type indicating how the body of a record is represented.
    pub trait BodyKind {
        fn content_length(&self) -> u64;
    }

    #[derive(Clone, Debug, Default, PartialEq)]
    /// An associated type indicating the body is buffered within the record.
    pub struct BufferedBody(pub Vec<u8>);
    impl BodyKind for BufferedBody {
        fn content_length(&self) -> u64 {
            self.0.len() as u64
        }
    }

    /// An associated type indicating the body is streamed from a reader.
    pub struct StreamingBody<'t, T: AsyncRead + Unpin + 't>(&'t mut T, &'t mut u64);
    impl<'t, T: AsyncRead + Unpin + 't> StreamingBody<'t, T> {
        pub(crate) fn new(stream: &'t mut T, max_len: &'t mut u64) -> StreamingBody<'t, T> {
            StreamingBody(stream, max_len)
        }
    }
    impl<'t, T: AsyncRead + Unpin + 't> BodyKind for StreamingBody<'t, T> {
        fn content_length(&self) -> u64 {
            *self.1
        }
    }

    impl<'t, T: AsyncRead + Unpin + 't> AsyncRead for StreamingBody<'t, T> {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            data: &mut [u8],
        ) -> Poll<std::io::Result<usize>> {
            let this = self.get_mut();
            let max_read = std::cmp::min(data.len() as u64, *this.1) as usize;
            if max_read == 0 {
                return Poll::Ready(Ok(0));
            }
            let n = ready!(Pin::new(&mut *this.0).poll_read(cx, &mut data[..max_read]))?;
            *this.1 -= n as u64;
            Poll::Ready(Ok(n))
        }
    }

    #[derive(Clone, Copy, Debug, Default)]
    /// An associated type indicated the record has a zero-length body.
    pub struct EmptyBody();
    impl BodyKind for EmptyBody {
        fn content_length(&self) -> u64 {
            0
        }
    }
}

/// A header block of a single WARC record as parsed from a data stream.
///
/// It is guaranteed to be well-formed, but may not be valid according to the specification.
///
/// Use the `Display` trait to generate the formatted representation.
#[derive(Clone, Debug, PartialEq)]
pub struct RawRecordHeader {
    /// The WARC standard version this record reports conformance to.
    pub version: String,
    /// All headers that are part of this record.
    pub headers: HashMap<WarcHeader, Vec<u8>>,
}

impl AsRef<HashMap<WarcHeader, Vec<u8>>> for RawRecordHeader {
    fn as_ref(&self) -> &HashMap<WarcHeader, Vec<u8>> {
        &self.headers
    }
}

impl AsMut<HashMap<WarcHeader, Vec<u8>>> for RawRecordHeader {
    fn as_mut(&mut self) -> &mut HashMap<WarcHeader, Vec<u8>> {
        &mut self.headers
    }
}

/// Removes a required header, failing if it is absent.
fn take_header(headers: &mut RawRecordHeader, header: WarcHeader) -> Result<Vec<u8>, WarcError> {
    headers
        .as_mut()
        .remove(&header)
        .ok_or(WarcError::MissingHeader(header))
}

/// Removes a required header as a string, failing if it is absent or not UTF-8.
fn take_utf8(headers: &mut RawRecordHeader, header: WarcHeader) -> Result<String, WarcError> {
    let value = take_header(headers, header.clone())?;
    String::from_utf8(value)
        .map_err(|_| WarcError::MalformedHeader(header, "not a UTF-8 string".to_string()))
}

impl std::convert::TryFrom<RawRecordHeader> for Record<EmptyBody> {
    type Error = WarcError;
    fn try_from(mut headers: RawRecordHeader) -> Result<Self, WarcError> {
        // The body determines the content length, so the value only has to be present.
        take_header(&mut headers, WarcHeader::ContentLength)?;
        let record_type = RecordType::from(take_utf8(&mut headers, WarcHeader::WarcType)?);
        let record_id = take_utf8(&mut headers, WarcHeader::RecordID)?;
        let record_date =
            Record::<BufferedBody>::parse_record_date(&take_utf8(&mut headers, WarcHeader::Date)?)?;

        let truncated_type = headers
            .as_mut()
            .remove(&WarcHeader::Truncated)
            .map(|value| TruncatedType::from(String::from_utf8_lossy(&value)));

        Ok(Record {
            headers,
            record_date,
            record_id,
            record_type,
            truncated_type,
            body: EmptyBody(),
        })
    }
}

impl std::fmt::Display for RawRecordHeader {
    fn fmt(&self, w: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        writeln!(w, "WARC/{}", self.version)?;
        for (key, value) in self.as_ref().iter() {
            writeln!(w, "{}: {}", key, String::from_utf8_lossy(value))?;
        }
        writeln!(w)?;

        Ok(())
    }
}

fn push_version_line(out: &mut Vec<u8>, version: &str) {
    out.extend_from_slice(b"WARC/");
    out.extend_from_slice(version.as_bytes());
    out.extend_from_slice(b"\r\n");
}

fn push_header(out: &mut Vec<u8>, name: &str, value: &[u8]) {
    out.extend_from_slice(name.as_bytes());
    out.extend_from_slice(b": ");
    out.extend_from_slice(value);
    out.extend_from_slice(b"\r\n");
}

impl RawRecordHeader {
    /// Appends the serialized header block, including the empty line that ends it, to `out`.
    pub(crate) fn write_header_block(&self, out: &mut Vec<u8>) {
        push_version_line(out, &self.version);
        for (header, value) in self.headers.iter() {
            push_header(out, header.as_str(), value);
        }
        out.extend_from_slice(b"\r\n");
    }
}

/// A builder for WARC records from data.
#[derive(Default)]
pub struct RecordBuilder {
    value: Record<BufferedBody>,
    broken_headers: HashMap<WarcHeader, Vec<u8>>,
    last_error: Option<WarcError>,
}

// HACK: std::io::Error doesn't implement Clone, this is the next best thing
// see: https://github.com/rust-lang/rust/issues/24135
impl Clone for RecordBuilder {
    fn clone(&self) -> Self {
        let err: Option<&WarcError> = self.last_error.as_ref();
        let last_error: Option<WarcError> = err.map(|err| match err {
            WarcError::ReadData(e) => WarcError::ReadData(std::io::Error::from(e.kind())),
            WarcError::ParseHeaders(e) => WarcError::ParseHeaders(e.clone()),
            WarcError::MissingHeader(e) => WarcError::MissingHeader(e.clone()),
            WarcError::MalformedHeader(h, e) => WarcError::MalformedHeader(h.clone(), e.clone()),
            WarcError::ReadOverflow => WarcError::ReadOverflow,
            WarcError::UnexpectedEOB => WarcError::UnexpectedEOB,
        });
        RecordBuilder {
            value: self.value.clone(),
            broken_headers: self.broken_headers.clone(),
            last_error,
        }
    }
}

/// A single WARC record.
///
/// A record can be constructed by a `RecordBuilder`, or by reading from a stream.
///
/// The associated type `T` indicates the representation of this record's body.
///
/// A record is guaranteed to be valid according to the specification it conforms to, except:
/// * The validity of the WARC-Record-ID header is not checked
/// * Date information not in the UTC timezone will be silently converted to UTC
///
/// Use the `Display` trait to generate the formatted representation.
#[derive(Clone, Debug, PartialEq)]
pub struct Record<T: BodyKind> {
    // NB: invariant: does not contain the headers stored in the struct
    headers: RawRecordHeader,
    record_date: DateTime<Utc>,
    record_id: String,
    record_type: RecordType,
    truncated_type: Option<TruncatedType>,
    body: T,
}

impl<T: BodyKind> Record<T> {
    /// Generate and return a new value suitable for use in the WARC-Record-ID header.
    ///
    /// # Compatibility
    /// The standard only places a small number of constraints on this field:
    /// 1. This value is globally unique "for its period of use"
    /// 1. This value is a valid URI
    /// 1. This value "clearly indicate\[s\] a documented and registered scheme to which it conforms."
    ///
    /// These guarantees will be upheld by all generated outputs, where the "period of use" is
    /// presumed to be indefinite and unlimited.
    ///
    /// However, any *specific algorithm* used to generate values is **not** part of the crate's
    /// public API for purposes of semantic versioning.
    ///
    /// # Implementation
    /// The current implementation generates random values based on UUID version 4.
    ///
    pub fn generate_record_id() -> String {
        format!("<{}>", Uuid::new_v4().urn())
    }

    fn parse_content_length(len: &str) -> Result<u64, WarcError> {
        (len).parse::<u64>().map_err(|_| {
            WarcError::MalformedHeader(
                WarcHeader::ContentLength,
                "not an integer between 0 and 2^64-1".to_string(),
            )
        })
    }

    fn parse_record_date(date: &str) -> Result<DateTime<Utc>, WarcError> {
        DateTime::parse_from_rfc3339(date)
            .map_err(|_| {
                WarcError::MalformedHeader(
                    WarcHeader::Date,
                    "not an ISO 8601 datestamp".to_string(),
                )
            })
            .map(|date| date.into())
    }

    /// Return the WARC version string of this record.
    pub fn warc_version(&self) -> &str {
        &self.headers.version
    }

    /// Set the WARC version string of this record.
    pub fn set_warc_version<S: Into<String>>(&mut self, id: S) {
        self.headers.version = id.into();
    }

    /// Return the WARC-Record-ID header for this record.
    pub fn warc_id(&self) -> &str {
        &self.record_id
    }

    /// Set the WARC-Record-ID header for this record.
    ///
    /// Note that this value is **not** checked for validity.
    pub fn set_warc_id<S: Into<String>>(&mut self, id: S) {
        self.record_id = id.into();
    }

    /// Return the WARC-Type header for this record.
    pub fn warc_type(&self) -> &RecordType {
        &self.record_type
    }

    /// Set the WARC-Type header for this record.
    pub fn set_warc_type(&mut self, type_: RecordType) {
        self.record_type = type_;
    }

    /// Return the WARC-Date header for this record.
    pub fn date(&self) -> &DateTime<Utc> {
        &self.record_date
    }

    /// Set the WARC-Date header for this record.
    pub fn set_date(&mut self, date: DateTime<Utc>) {
        self.record_date = date;
    }

    /// Return the WARC-Truncated header for this record.
    pub fn truncated_type(&self) -> &Option<TruncatedType> {
        &self.truncated_type
    }

    /// Set the WARC-Truncated header for this record.
    pub fn set_truncated_type(&mut self, truncated_type: TruncatedType) {
        self.truncated_type = Some(truncated_type);
    }

    /// Remove the WARC-Truncated header for this record.
    pub fn clear_truncated_type(&mut self) {
        self.truncated_type = None;
    }

    /// Return the WARC header requested if present in this record, or `None`.
    ///
    /// Values that are not valid UTF-8 are converted lossily.
    pub fn header(&self, header: WarcHeader) -> Option<Cow<'_, str>> {
        match &header {
            WarcHeader::ContentLength => Some(Cow::Owned(self.body.content_length().to_string())),
            WarcHeader::RecordID => Some(Cow::Borrowed(self.warc_id())),
            WarcHeader::WarcType => Some(Cow::Borrowed(self.record_type.as_str())),
            WarcHeader::Date => Some(Cow::Owned(
                self.date().to_rfc3339_opts(SecondsFormat::Secs, true),
            )),
            WarcHeader::Truncated => self
                .truncated_type
                .as_ref()
                .map(|truncated_type| Cow::Borrowed(truncated_type.as_str())),
            _ => self
                .headers
                .as_ref()
                .get(&header)
                .map(|h| String::from_utf8_lossy(h)),
        }
    }

    /// Set a WARC header in this record, returning the previous value if present.
    ///
    /// # Errors
    ///
    /// If setting a header whose value has a well-formedness test, an error is returned if the
    /// value is not well-formed.
    pub fn set_header<V>(
        &mut self,
        header: WarcHeader,
        value: V,
    ) -> Result<Option<String>, WarcError>
    where
        V: Into<String>,
    {
        let value = value.into();
        match &header {
            WarcHeader::Date => {
                let old_date = std::mem::replace(
                    &mut self.record_date,
                    Record::<T>::parse_record_date(&value)?,
                );
                Ok(Some(old_date.to_rfc3339_opts(SecondsFormat::Secs, true)))
            }
            WarcHeader::RecordID => Ok(Some(std::mem::replace(&mut self.record_id, value))),
            WarcHeader::WarcType => {
                let old_type = std::mem::replace(&mut self.record_type, RecordType::from(&value));
                Ok(Some(old_type.to_string()))
            }
            WarcHeader::Truncated => {
                let old_type = self.truncated_type.replace(TruncatedType::from(&value));
                Ok(old_type.map(|old| old.to_string()))
            }
            WarcHeader::ContentLength => {
                if Record::<T>::parse_content_length(&value)? != self.body.content_length() {
                    Err(WarcError::MalformedHeader(
                        WarcHeader::ContentLength,
                        "content length != body size".to_string(),
                    ))
                } else {
                    Ok(Some(value))
                }
            }
            _ => Ok(self
                .headers
                .as_mut()
                .insert(header, Vec::from(value))
                .map(|v| {
                    String::from_utf8(v)
                        .unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
                })),
        }
    }

    /// Return the Content-Length header for this record.
    ///
    /// This value is guaranteed to match the actual length of the body.
    pub fn content_length(&self) -> u64 {
        self.body.content_length()
    }

    fn replace_body_kind<U: BodyKind>(self, body: U) -> Record<U> {
        Record {
            headers: self.headers,
            record_date: self.record_date,
            record_id: self.record_id,
            record_type: self.record_type,
            truncated_type: self.truncated_type,
            body,
        }
    }

    /// Returns the headers stored as dedicated fields, in their raw form.
    fn stored_fields(&self) -> Vec<(WarcHeader, Vec<u8>)> {
        let mut fields = vec![
            (
                WarcHeader::ContentLength,
                self.content_length().to_string().into_bytes(),
            ),
            (
                WarcHeader::WarcType,
                self.record_type.to_string().into_bytes(),
            ),
            (WarcHeader::RecordID, self.record_id.clone().into_bytes()),
            (
                WarcHeader::Date,
                self.record_date
                    .to_rfc3339_opts(SecondsFormat::Secs, true)
                    .into_bytes(),
            ),
        ];
        if let Some(truncated_type) = &self.truncated_type {
            fields.push((
                WarcHeader::Truncated,
                truncated_type.to_string().into_bytes(),
            ));
        }
        fields
    }

    /// Appends the serialized header block, including the empty line that ends it, to `out`.
    ///
    /// Produces the same headers as `to_raw_header` without cloning them.
    pub(crate) fn write_header_block(&self, out: &mut Vec<u8>) {
        push_version_line(out, &self.headers.version);
        push_header(
            out,
            WarcHeader::WarcType.as_str(),
            self.record_type.as_str().as_bytes(),
        );
        push_header(
            out,
            WarcHeader::RecordID.as_str(),
            self.record_id.as_bytes(),
        );
        push_header(
            out,
            WarcHeader::Date.as_str(),
            self.record_date
                .to_rfc3339_opts(SecondsFormat::Secs, true)
                .as_bytes(),
        );
        push_header(
            out,
            WarcHeader::ContentLength.as_str(),
            self.content_length().to_string().as_bytes(),
        );
        if let Some(truncated_type) = &self.truncated_type {
            push_header(
                out,
                WarcHeader::Truncated.as_str(),
                truncated_type.as_str().as_bytes(),
            );
        }
        for (header, value) in self.headers.headers.iter() {
            push_header(out, header.as_str(), value);
        }
        out.extend_from_slice(b"\r\n");
    }
}

fn with_stored_fields(
    mut headers: RawRecordHeader,
    fields: Vec<(WarcHeader, Vec<u8>)>,
) -> RawRecordHeader {
    for (header, value) in fields {
        let previous = headers.as_mut().insert(header, value);
        debug_assert!(
            previous.is_none(),
            "invariant violation: raw struct contains externally stored fields"
        );
    }
    headers
}

impl Record<EmptyBody> {
    /// Create a new empty record with default values.
    ///
    /// Using a `RecordBuilder` is more efficient when creating records from known data.
    ///
    /// The record returned contains an empty body, and the following fields:
    /// * WARC-Record-ID: generated by `generate_record_id()`
    /// * WARC-Date: the current moment in time
    /// * WARC-Type: resource
    /// * WARC-Content-Length: 0
    pub fn new() -> Record<EmptyBody> {
        Record::default()
    }

    /// Add a known body to this record, transforming it into a buffered body record.
    pub fn add_body<B: Into<Vec<u8>>>(self, body: B) -> Record<BufferedBody> {
        self.replace_body_kind(BufferedBody(body.into()))
    }

    /// Add a streaming body to this record, whose expected size may not match the actual stream
    /// length.
    pub fn add_fixed_stream<'r, R: AsyncRead + Unpin + 'r>(
        self,
        stream: &'r mut R,
        len: &'r mut u64,
    ) -> Record<StreamingBody<'r, R>> {
        self.replace_body_kind(StreamingBody::new(stream, len))
    }
}

impl Record<BufferedBody> {
    /// Create a new empty record with a known body.
    ///
    /// Using a `RecordBuilder` is more efficient when creating records from known data.
    ///
    /// The record returned contains the passed body buffer, and the following fields:
    /// * WARC-Record-ID: generated by `generate_record_id()`
    /// * WARC-Date: the current moment in time
    /// * WARC-Type: resource
    /// * WARC-Content-Length: `body.len()`
    pub fn with_body<B: Into<Vec<u8>>>(body: B) -> Record<BufferedBody> {
        Record {
            body: BufferedBody(body.into()),
            ..Record::default()
        }
    }

    /// Strip the body from this record.
    pub fn strip_body(self) -> Record<EmptyBody> {
        self.replace_body_kind(EmptyBody())
    }

    /// Return the body of this record.
    pub fn body(&self) -> &[u8] {
        self.body.0.as_slice()
    }

    /// Return a reference to mutate the body of this record, but without changing its length.
    ///
    /// To update the body of the record or change its length, use the `replace_body` method
    /// instead.
    pub fn body_mut(&mut self) -> &mut [u8] {
        self.body.0.as_mut_slice()
    }

    /// Replace the body of this record with the given body.
    pub fn replace_body<V: Into<Vec<u8>>>(&mut self, new_body: V) {
        self.body.0 = new_body.into();
    }

    /// Transform this record into a raw record containing the same data.
    pub fn into_raw_parts(self) -> (RawRecordHeader, Vec<u8>) {
        let fields = self.stored_fields();
        (with_stored_fields(self.headers, fields), self.body.0)
    }

    /// Return the raw header block of this record without consuming it.
    pub(crate) fn to_raw_header(&self) -> RawRecordHeader {
        with_stored_fields(self.headers.clone(), self.stored_fields())
    }
}

impl<'t, T: AsyncRead + Unpin + 't> Record<StreamingBody<'t, T>> {
    /// Returns a record with a buffered body by collecting the streaming body.
    ///
    /// # Errors
    ///
    /// This method can fail if the underlying stream returns an error. If this happens, the
    /// state of the stream is not guaranteed.
    pub async fn into_buffered(mut self) -> std::io::Result<Record<BufferedBody>> {
        let mut body = Vec::with_capacity(std::cmp::min(
            self.body.content_length(),
            MAX_BODY_PREALLOCATION,
        ) as usize);
        self.body.read_to_end(&mut body).await?;
        Ok(self.replace_body_kind(BufferedBody(body)))
    }
}

impl<'t, T: AsyncRead + Unpin + 't> AsyncRead for Record<StreamingBody<'t, T>> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        dst: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().body).poll_read(cx, dst)
    }
}

impl<T: BodyKind + Default> Default for Record<T> {
    fn default() -> Record<T> {
        Record {
            headers: RawRecordHeader {
                version: "1.0".to_string(),
                headers: HashMap::new(),
            },
            record_date: Utc::now(),
            record_id: Self::generate_record_id(),
            record_type: RecordType::Resource,
            truncated_type: None,
            body: T::default(),
        }
    }
}

impl fmt::Display for Record<BufferedBody> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Record({}, {:?})", self.to_raw_header(), self.body())
    }
}
impl fmt::Display for Record<EmptyBody> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Record({:?}, Empty)", self.headers)
    }
}

impl RecordBuilder {
    /// Set the body of the record under construction.
    pub fn body(mut self, body: Vec<u8>) -> Self {
        self.value.replace_body(body);

        self
    }

    /// Set the record date header of the record under construction.
    pub fn date(mut self, date: DateTime<Utc>) -> Self {
        self.value.set_date(date);

        self
    }

    /// Set the record ID header of the record under construction.
    pub fn warc_id<S: Into<String>>(mut self, id: S) -> Self {
        self.value.set_warc_id(id);

        self
    }

    /// Set the WARC version of the record under construction.
    pub fn version(mut self, version: String) -> Self {
        self.value.set_warc_version(version);

        self
    }

    /// Set the WARC record type header field of the record under construction.
    pub fn warc_type(mut self, warc_type: RecordType) -> Self {
        self.value.set_warc_type(warc_type);

        self
    }

    /// Set the truncated type header of the record under construction.
    pub fn truncated_type(mut self, trunc_type: TruncatedType) -> Self {
        self.value.set_truncated_type(trunc_type);

        self
    }

    /// Create or replace an arbitrary header of the record under construction.
    pub fn header<V: Into<Vec<u8>>>(mut self, key: WarcHeader, value: V) -> Self {
        let value = value.into();
        let result = match std::str::from_utf8(&value) {
            Ok(string) => self.value.set_header(key.clone(), string).map(|_| ()),
            Err(_) => Err(WarcError::MalformedHeader(
                key.clone(),
                "not a UTF-8 string".to_string(),
            )),
        };

        match result {
            Ok(()) => {
                self.broken_headers.remove(&key);
            }
            Err(e) => {
                self.broken_headers.insert(key, value);
                self.last_error = Some(e);
            }
        }

        self
    }

    /// Build a raw record header from the data collected in this builder.
    ///
    /// A body set in this builder will be returned raw.
    pub fn build_raw(self) -> (RawRecordHeader, Vec<u8>) {
        let RecordBuilder {
            value,
            broken_headers,
            ..
        } = self;
        let (mut headers, body) = value.into_raw_parts();
        headers.as_mut().extend(broken_headers);

        (headers, body)
    }

    /// Build a record from the data collected in this builder.
    pub fn build(self) -> Result<Record<BufferedBody>, WarcError> {
        let RecordBuilder {
            value,
            broken_headers,
            last_error,
        } = self;

        if let Some(e) = last_error {
            Err(e)
        } else {
            debug_assert!(
                broken_headers.is_empty(),
                "invariant violation: broken headers without last error"
            );
            Ok(value)
        }
    }
}

#[cfg(test)]
mod record_tests {
    use crate::header::WarcHeader;
    use crate::{BufferedBody, Record, RecordType};

    use chrono::prelude::*;

    #[test]
    fn default() {
        let before = Utc::now();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let record = Record::<BufferedBody>::default();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let after = Utc::now();
        assert_eq!(record.content_length(), 0);
        assert_eq!(record.warc_version(), "1.0");
        assert_eq!(record.warc_type(), &RecordType::Resource);
        assert!(record.date() > &before);
        assert!(record.date() < &after);
    }

    #[test]
    fn impl_eq() {
        let record1 = Record::<BufferedBody>::default();
        let record2 = record1.clone();
        assert_eq!(record1, record2);
    }

    #[test]
    fn body() {
        let mut record = Record::<BufferedBody>::default();
        assert_eq!(record.content_length(), 0);
        assert!(record.body().is_empty());
        record.replace_body(b"hello!!".to_vec());
        assert_eq!(record.content_length(), 7);
        assert_eq!(record.body(), b"hello!!");
        record.body_mut().copy_from_slice(b"goodbye");
        assert_eq!(record.content_length(), 7);
        assert_eq!(record.body(), b"goodbye");
    }

    #[test]
    fn add_header() {
        let mut record = Record::<BufferedBody>::default();
        assert!(record.header(WarcHeader::TargetURI).is_none());
        assert!(record
            .set_header(WarcHeader::TargetURI, "https://www.rust-lang.org")
            .unwrap()
            .is_none());
        assert_eq!(
            record.header(WarcHeader::TargetURI).unwrap(),
            "https://www.rust-lang.org"
        );
        assert_eq!(
            record
                .set_header(WarcHeader::TargetURI, "https://docs.rs")
                .unwrap()
                .unwrap(),
            "https://www.rust-lang.org"
        );
        assert_eq!(
            record.header(WarcHeader::TargetURI).unwrap(),
            "https://docs.rs"
        );
    }

    #[test]
    fn set_header_override_content_length() {
        let mut record = Record::<BufferedBody>::default();
        assert_eq!(record.header(WarcHeader::ContentLength).unwrap(), "0");
        assert!(record
            .set_header(WarcHeader::ContentLength, "really short")
            .is_err());
        assert!(record.set_header(WarcHeader::ContentLength, "50").is_err());
        assert_eq!(
            record
                .set_header(WarcHeader::ContentLength, "0")
                .unwrap()
                .unwrap(),
            "0"
        );
    }

    #[test]
    fn set_header_override_warc_date() {
        let mut record = Record::<BufferedBody>::default();
        let old_date = record.date().to_rfc3339_opts(SecondsFormat::Secs, true);
        assert_eq!(record.header(WarcHeader::Date).unwrap(), old_date);
        assert!(record.set_header(WarcHeader::Date, "yesterday").is_err());
        assert_eq!(
            record
                .set_header(WarcHeader::Date, "2020-07-21T22:00:00Z")
                .unwrap()
                .unwrap(),
            old_date
        );
        assert_eq!(
            record.header(WarcHeader::Date).unwrap(),
            "2020-07-21T22:00:00Z"
        );
    }

    #[test]
    fn set_header_override_warc_record_id() {
        let mut record = Record::<BufferedBody>::default();
        let old_id = record.warc_id().to_string();
        assert_eq!(
            record.header(WarcHeader::RecordID).unwrap(),
            old_id.as_str()
        );
        assert_eq!(
            record
                .set_header(WarcHeader::RecordID, "urn:http:www.rust-lang.org")
                .unwrap()
                .unwrap(),
            old_id.as_str()
        );
        assert_eq!(
            record.header(WarcHeader::RecordID).unwrap(),
            "urn:http:www.rust-lang.org"
        );
    }

    #[test]
    fn set_header_override_warc_type() {
        let mut record = Record::<BufferedBody>::default();
        assert_eq!(record.header(WarcHeader::WarcType).unwrap(), "resource");
        assert_eq!(
            record
                .set_header(WarcHeader::WarcType, "revisit")
                .unwrap()
                .unwrap(),
            "resource"
        );
        assert_eq!(record.header(WarcHeader::WarcType).unwrap(), "revisit");
    }
}

#[cfg(test)]
mod raw_tests {
    use crate::header::WarcHeader;
    use crate::{EmptyBody, RawRecordHeader, Record, RecordType, TruncatedType};

    use std::collections::HashMap;
    use std::convert::TryFrom;

    #[test]
    fn create() {
        let headers = RawRecordHeader {
            version: "1.0".to_owned(),
            headers: HashMap::new(),
        };

        assert_eq!(headers.as_ref().len(), 0);
    }

    #[test]
    fn create_with_headers() {
        let headers = RawRecordHeader {
            version: "1.0".to_owned(),
            headers: vec![(
                WarcHeader::WarcType,
                RecordType::WarcInfo.to_string().into_bytes(),
            )]
            .into_iter()
            .collect(),
        };

        assert_eq!(headers.as_ref().len(), 1);
    }

    #[test]
    fn verify_ok() {
        let headers = RawRecordHeader {
            version: "1.0".to_owned(),
            headers: vec![
                (WarcHeader::WarcType, b"dunno".to_vec()),
                (WarcHeader::ContentLength, b"5".to_vec()),
                (
                    WarcHeader::RecordID,
                    b"<urn:test:basic-record:record-0>".to_vec(),
                ),
                (WarcHeader::Date, b"2020-07-08T02:52:55Z".to_vec()),
            ]
            .into_iter()
            .collect(),
        };

        assert!(Record::<EmptyBody>::try_from(headers).is_ok());
    }

    #[test]
    fn verify_missing_type() {
        let headers = RawRecordHeader {
            version: "1.0".to_owned(),
            headers: vec![
                (WarcHeader::ContentLength, b"5".to_vec()),
                (
                    WarcHeader::RecordID,
                    b"<urn:test:basic-record:record-0>".to_vec(),
                ),
                (WarcHeader::Date, b"2020-07-08T02:52:55Z".to_vec()),
            ]
            .into_iter()
            .collect(),
        };

        assert!(Record::<EmptyBody>::try_from(headers).is_err());
    }

    #[test]
    fn verify_missing_content_length() {
        let headers = RawRecordHeader {
            version: "1.0".to_owned(),
            headers: vec![
                (WarcHeader::WarcType, b"dunno".to_vec()),
                (
                    WarcHeader::RecordID,
                    b"<urn:test:basic-record:record-0>".to_vec(),
                ),
                (WarcHeader::Date, b"2020-07-08T02:52:55Z".to_vec()),
            ]
            .into_iter()
            .collect(),
        };

        assert!(Record::<EmptyBody>::try_from(headers).is_err());
    }

    #[test]
    fn verify_missing_record_id() {
        let headers = RawRecordHeader {
            version: "1.0".to_owned(),
            headers: vec![
                (WarcHeader::WarcType, b"dunno".to_vec()),
                (WarcHeader::ContentLength, b"5".to_vec()),
                (WarcHeader::Date, b"2020-07-08T02:52:55Z".to_vec()),
            ]
            .into_iter()
            .collect(),
        };

        assert!(Record::<EmptyBody>::try_from(headers).is_err());
    }

    #[test]
    fn verify_missing_date() {
        let headers = RawRecordHeader {
            version: "1.0".to_owned(),
            headers: vec![
                (WarcHeader::WarcType, b"dunno".to_vec()),
                (WarcHeader::ContentLength, b"5".to_vec()),
                (
                    WarcHeader::RecordID,
                    b"<urn:test:basic-record:record-0>".to_vec(),
                ),
            ]
            .into_iter()
            .collect(),
        };

        assert!(Record::<EmptyBody>::try_from(headers).is_err());
    }

    fn truncated_record() -> Record<EmptyBody> {
        let headers = RawRecordHeader {
            version: "1.0".to_owned(),
            headers: vec![
                (WarcHeader::WarcType, b"response".to_vec()),
                (WarcHeader::ContentLength, b"0".to_vec()),
                (
                    WarcHeader::RecordID,
                    b"<urn:test:basic-record:record-0>".to_vec(),
                ),
                (WarcHeader::Date, b"2020-07-08T02:52:55Z".to_vec()),
                (WarcHeader::Truncated, b"length".to_vec()),
            ]
            .into_iter()
            .collect(),
        };
        Record::<EmptyBody>::try_from(headers).unwrap()
    }

    #[test]
    fn verify_truncated_is_extracted() {
        let record = truncated_record();

        assert_eq!(record.truncated_type(), &Some(TruncatedType::Length));
        assert_eq!(
            record.header(WarcHeader::Truncated).as_deref(),
            Some("length")
        );
    }

    #[test]
    fn verify_truncated_round_trips_after_update() {
        let mut record = truncated_record().add_body(vec![]);
        record.set_truncated_type(TruncatedType::Time);

        let (headers, _) = record.into_raw_parts();

        assert_eq!(
            headers.as_ref().get(&WarcHeader::Truncated),
            Some(&b"time".to_vec())
        );
    }

    #[test]
    fn verify_display() {
        let header_entries = vec![
            (WarcHeader::WarcType, b"dunno".to_vec()),
            (WarcHeader::Date, b"2024-01-01T00:00:00Z".to_vec()),
        ];

        let headers = RawRecordHeader {
            version: "1.0".to_owned(),
            headers: header_entries.into_iter().collect(),
        };

        let output = headers.to_string();

        let expected_lines = [
            "WARC/1.0",
            "warc-type: dunno",
            "warc-date: 2024-01-01T00:00:00Z",
            "",
        ];
        let actual_lines: Vec<_> = output.lines().collect();

        let mut expected_headers: Vec<_> = expected_lines[1..expected_lines.len() - 1].to_vec();
        expected_headers.sort();

        let mut actual_headers: Vec<_> = actual_lines[1..actual_lines.len() - 1].to_vec();
        actual_headers.sort();

        // verify parts
        assert_eq!(actual_lines[0], expected_lines[0]); // WARC version
        assert_eq!(actual_headers, expected_headers); // headers (sorted)
        assert_eq!(actual_lines.last(), expected_lines.last()); // empty line
    }
}

#[cfg(test)]
mod builder_tests {
    use crate::header::WarcHeader;
    use crate::{
        BufferedBody, EmptyBody, RawRecordHeader, Record, RecordBuilder, RecordType, TruncatedType,
    };

    use std::convert::TryFrom;

    #[test]
    fn default() {
        let (headers, body) = RecordBuilder::default().build_raw();
        assert_eq!(headers.version, "1.0".to_string());
        assert_eq!(
            headers.as_ref().get(&WarcHeader::ContentLength).unwrap(),
            &b"0".to_vec()
        );
        assert!(body.is_empty());
        assert_eq!(
            RecordBuilder::default().build().unwrap().content_length(),
            0
        );
    }

    #[test]
    fn default_with_body() {
        let (headers, body) = RecordBuilder::default()
            .body(b"abcdef".to_vec())
            .build_raw();
        assert_eq!(headers.version, "1.0".to_string());
        assert_eq!(
            headers.as_ref().get(&WarcHeader::ContentLength).unwrap(),
            &b"6".to_vec()
        );
        assert_eq!(body.as_slice(), b"abcdef");
        assert_eq!(
            RecordBuilder::default()
                .body(b"abcdef".to_vec())
                .build()
                .unwrap()
                .content_length(),
            6
        );
    }

    #[test]
    fn impl_eq_raw() {
        let builder = RecordBuilder::default();
        let raw1 = builder.clone().build_raw();

        let raw2 = builder.build_raw();
        assert_eq!(raw1, raw2);
    }

    #[test]
    fn impl_eq_record() {
        let builder = RecordBuilder::default();
        let record1 = builder.clone().build().unwrap();

        let record2 = builder.build().unwrap();
        assert_eq!(record1, record2);
    }

    #[test]
    fn create_with_headers() {
        let headers = RawRecordHeader {
            version: "1.0".to_owned(),
            headers: vec![(
                WarcHeader::WarcType,
                RecordType::WarcInfo.to_string().into_bytes(),
            )]
            .into_iter()
            .collect(),
        };

        assert_eq!(headers.as_ref().len(), 1);
    }

    #[test]
    fn verify_ok() {
        let headers = RawRecordHeader {
            version: "1.0".to_owned(),
            headers: vec![
                (WarcHeader::WarcType, b"dunno".to_vec()),
                (WarcHeader::ContentLength, b"5".to_vec()),
                (
                    WarcHeader::RecordID,
                    b"<urn:test:basic-record:record-0>".to_vec(),
                ),
                (WarcHeader::Date, b"2020-07-08T02:52:55Z".to_vec()),
            ]
            .into_iter()
            .collect(),
        };

        assert!(Record::<EmptyBody>::try_from(headers).is_ok());
    }

    #[test]
    fn verify_content_length() {
        let mut builder = RecordBuilder::default().body(b"12345".to_vec());

        assert_eq!(
            builder
                .clone()
                .build()
                .unwrap()
                .into_raw_parts()
                .0
                .as_ref()
                .get(&WarcHeader::ContentLength)
                .unwrap(),
            &b"5".to_vec()
        );

        assert_eq!(
            builder
                .clone()
                .build_raw()
                .0
                .as_ref()
                .get(&WarcHeader::ContentLength)
                .unwrap(),
            &b"5".to_vec()
        );

        builder = builder.header(WarcHeader::ContentLength, "1");
        assert_eq!(
            builder
                .clone()
                .build_raw()
                .0
                .as_ref()
                .get(&WarcHeader::ContentLength)
                .unwrap(),
            &b"1".to_vec()
        );

        assert!(builder.build().is_err());
    }

    #[test]
    fn verify_build_record_type() {
        let builder1 = RecordBuilder::default().header(WarcHeader::WarcType, "request");
        let builder2 = builder1.clone().warc_type(RecordType::Request);

        let record1 = builder1.build().unwrap();
        let record2 = builder2.build().unwrap();

        assert_eq!(record1, record2);
        assert_eq!(
            record1
                .into_raw_parts()
                .0
                .as_ref()
                .get(&WarcHeader::WarcType),
            Some(&b"request".to_vec())
        );
    }

    #[test]
    fn verify_build_date() {
        const DATE_STRING_0: &str = "2020-07-08T02:52:55Z";
        const DATE_STRING_1: &[u8] = b"2020-07-18T02:12:45Z";

        let mut builder = RecordBuilder::default();
        builder = builder.date(Record::<BufferedBody>::parse_record_date(DATE_STRING_0).unwrap());

        let record = builder.clone().build().unwrap();
        assert_eq!(
            record
                .into_raw_parts()
                .0
                .as_ref()
                .get(&WarcHeader::Date)
                .unwrap(),
            &DATE_STRING_0.as_bytes()
        );
        assert_eq!(
            builder
                .clone()
                .build_raw()
                .0
                .as_ref()
                .get(&WarcHeader::Date)
                .unwrap(),
            &DATE_STRING_0.as_bytes()
        );

        builder = builder.header(WarcHeader::Date, DATE_STRING_1.to_vec());
        let record = builder.clone().build().unwrap();
        assert_eq!(
            record
                .into_raw_parts()
                .0
                .as_ref()
                .get(&WarcHeader::Date)
                .unwrap(),
            &DATE_STRING_1.to_vec()
        );
        assert_eq!(
            builder
                .clone()
                .build_raw()
                .0
                .as_ref()
                .get(&WarcHeader::Date)
                .unwrap(),
            &DATE_STRING_1.to_vec()
        );

        let builder = builder.header(WarcHeader::Date, b"not-a-dayTor:a:time".to_vec());
        assert!(builder.build().is_err());
    }

    #[test]
    fn verify_build_record_id() {
        const RECORD_ID_0: &[u8] = b"<urn:test:verify-build-id:record-0>";
        const RECORD_ID_1: &[u8] = b"<urn:test:verify-build-id:record-1>";

        let mut builder = RecordBuilder::default();
        builder = builder.warc_id(std::str::from_utf8(RECORD_ID_0).unwrap());

        let record = builder.clone().build().unwrap();
        assert_eq!(
            record
                .into_raw_parts()
                .0
                .as_ref()
                .get(&WarcHeader::RecordID)
                .unwrap(),
            &RECORD_ID_0.to_vec()
        );
        assert_eq!(
            builder
                .clone()
                .build_raw()
                .0
                .as_ref()
                .get(&WarcHeader::RecordID)
                .unwrap(),
            &RECORD_ID_0.to_vec()
        );

        let builder = builder.header(WarcHeader::RecordID, RECORD_ID_1.to_vec());
        let record = builder.clone().build().unwrap();
        assert_eq!(
            record
                .into_raw_parts()
                .0
                .as_ref()
                .get(&WarcHeader::RecordID)
                .unwrap(),
            &RECORD_ID_1.to_vec()
        );
        assert_eq!(
            builder
                .clone()
                .build_raw()
                .0
                .as_ref()
                .get(&WarcHeader::RecordID)
                .unwrap(),
            &RECORD_ID_1.to_vec()
        );
    }

    #[test]
    fn verify_build_truncated_type() {
        const TRUNCATED_TYPE_0: &[u8] = b"length";
        const TRUNCATED_TYPE_1: &[u8] = b"disconnect";

        let mut builder = RecordBuilder::default();
        builder = builder.truncated_type(TruncatedType::Length);

        let record = builder.clone().build().unwrap();
        assert_eq!(
            record
                .into_raw_parts()
                .0
                .as_ref()
                .get(&WarcHeader::Truncated)
                .unwrap(),
            &TRUNCATED_TYPE_0.to_vec()
        );
        assert_eq!(
            builder
                .clone()
                .build_raw()
                .0
                .as_ref()
                .get(&WarcHeader::Truncated)
                .unwrap(),
            &TRUNCATED_TYPE_0.to_vec()
        );

        builder = builder.header(WarcHeader::Truncated, "disconnect");
        let record = builder.clone().build().unwrap();
        assert_eq!(
            record
                .into_raw_parts()
                .0
                .as_ref()
                .get(&WarcHeader::Truncated)
                .unwrap(),
            &TRUNCATED_TYPE_1.to_vec()
        );
        assert_eq!(
            builder
                .clone()
                .build_raw()
                .0
                .as_ref()
                .get(&WarcHeader::Truncated)
                .unwrap(),
            &TRUNCATED_TYPE_1.to_vec()
        );

        builder = builder.header(WarcHeader::Truncated, "foreign-intervention");
        assert_eq!(
            builder
                .clone()
                .build()
                .unwrap()
                .into_raw_parts()
                .0
                .as_ref()
                .get(&WarcHeader::Truncated)
                .unwrap()
                .as_slice(),
            &b"foreign-intervention"[..]
        );

        assert_eq!(
            builder
                .clone()
                .build_raw()
                .0
                .as_ref()
                .get(&WarcHeader::Truncated)
                .unwrap()
                .as_slice(),
            &b"foreign-intervention"[..]
        );
    }
}
