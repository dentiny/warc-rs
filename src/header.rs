use std::fmt::Display;

#[cfg(feature = "with_serde")]
use serde::{Deserialize, Serialize};
/// Represents a WARC header defined by the standard.
///
/// All headers are camel-case versions of the standard names, with the hyphens removed.
#[allow(missing_docs)]
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
#[cfg_attr(feature = "with_serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "with_serde", serde(into = "String"))]
#[cfg_attr(feature = "with_serde", serde(from = "String"))]
pub enum WarcHeader {
    ContentLength,
    ContentType,
    BlockDigest,
    ConcurrentTo,
    Date,
    Filename,
    IdentifiedPayloadType,
    IPAddress,
    PayloadDigest,
    Profile,
    RecordID,
    RefersTo,
    SegmentNumber,
    SegmentOriginID,
    SegmentTotalLength,
    TargetURI,
    Truncated,
    WarcType,
    WarcInfoID,
    Unknown(String),
}

impl From<WarcHeader> for String {
    fn from(header: WarcHeader) -> Self {
        header.to_string()
    }
}

const KNOWN_HEADERS: [WarcHeader; 19] = [
    WarcHeader::ContentLength,
    WarcHeader::ContentType,
    WarcHeader::BlockDigest,
    WarcHeader::ConcurrentTo,
    WarcHeader::Date,
    WarcHeader::Filename,
    WarcHeader::IdentifiedPayloadType,
    WarcHeader::IPAddress,
    WarcHeader::PayloadDigest,
    WarcHeader::Profile,
    WarcHeader::RecordID,
    WarcHeader::RefersTo,
    WarcHeader::SegmentNumber,
    WarcHeader::SegmentOriginID,
    WarcHeader::SegmentTotalLength,
    WarcHeader::TargetURI,
    WarcHeader::Truncated,
    WarcHeader::WarcType,
    WarcHeader::WarcInfoID,
];

impl WarcHeader {
    /// Returns the lowercase header name.
    pub fn as_str(&self) -> &str {
        match self {
            WarcHeader::ContentLength => "content-length",
            WarcHeader::ContentType => "content-type",
            WarcHeader::BlockDigest => "warc-block-digest",
            WarcHeader::ConcurrentTo => "warc-concurrent-to",
            WarcHeader::Date => "warc-date",
            WarcHeader::Filename => "warc-filename",
            WarcHeader::IdentifiedPayloadType => "warc-identified-payload-type",
            WarcHeader::IPAddress => "warc-ip-address",
            WarcHeader::PayloadDigest => "warc-payload-digest",
            WarcHeader::Profile => "warc-profile",
            WarcHeader::RecordID => "warc-record-id",
            WarcHeader::RefersTo => "warc-refers-to",
            WarcHeader::SegmentNumber => "warc-segment-number",
            WarcHeader::SegmentOriginID => "warc-segment-origin-id",
            WarcHeader::SegmentTotalLength => "warc-segment-total-length",
            WarcHeader::TargetURI => "warc-target-uri",
            WarcHeader::Truncated => "warc-truncated",
            WarcHeader::WarcType => "warc-type",
            WarcHeader::WarcInfoID => "warc-warcinfo-id",
            WarcHeader::Unknown(ref string) => string,
        }
    }
}

impl Display for WarcHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<S: AsRef<str>> From<S> for WarcHeader {
    fn from(string: S) -> Self {
        let string = string.as_ref();
        KNOWN_HEADERS
            .iter()
            .find(|header| header.as_str().eq_ignore_ascii_case(string))
            .cloned()
            .unwrap_or_else(|| WarcHeader::Unknown(string.to_lowercase()))
    }
}

#[cfg(test)]
mod tests {
    use super::{WarcHeader, KNOWN_HEADERS};

    #[test]
    fn known_headers_round_trip_case_insensitively() {
        for header in KNOWN_HEADERS.iter() {
            assert_eq!(&WarcHeader::from(header.as_str()), header);
            assert_eq!(
                &WarcHeader::from(header.as_str().to_ascii_uppercase()),
                header
            );
        }
        assert_eq!(WarcHeader::from("WARC-Record-ID"), WarcHeader::RecordID);
    }

    #[test]
    fn unknown_headers_are_lowercased() {
        assert_eq!(
            WarcHeader::from("X-Custom-Header"),
            WarcHeader::Unknown("x-custom-header".to_string())
        );
    }
}
