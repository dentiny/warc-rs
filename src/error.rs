use std::error;
use std::fmt;

use crate::header::WarcHeader;

/// Header buffers can span many kilobytes, so only a prefix is shown in error messages.
const MAX_INPUT_PREVIEW_BYTES: usize = 64;

/// An error type returned by WARC header parsing.
#[derive(Debug)]
pub enum Error {
    /// An error occured identifing or parsing headers.
    ParseHeaders(nom::Err<(Vec<u8>, nom::error::ErrorKind)>),
    /// A header required by the standard is missing from the record. The record was well-formed,
    /// but invalid.
    MissingHeader(WarcHeader),
    /// A required header is not well-formed according to the standard.
    MalformedHeader(WarcHeader, String),
    /// The underlying read from the data source failed.
    ReadData(std::io::Error),
    /// More data was read than expected by the header metadata. The record was well-formed, but
    /// invalid.
    ReadOverflow,
    /// The end of the record's body was found unexpectedly.
    UnexpectedEOB,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::ParseHeaders(nom::Err::Incomplete(_)) => {
                write!(f, "Error parsing headers: incomplete input.")
            }
            Error::ParseHeaders(nom::Err::Error((input, kind)))
            | Error::ParseHeaders(nom::Err::Failure((input, kind))) => {
                let preview = &input[..input.len().min(MAX_INPUT_PREVIEW_BYTES)];
                write!(
                    f,
                    "Error parsing headers: {} at {:?}",
                    kind.description(),
                    String::from_utf8_lossy(preview)
                )?;
                if input.len() > preview.len() {
                    write!(f, " ({} more bytes)", input.len() - preview.len())?;
                }
                Ok(())
            }
            Error::MissingHeader(ref h) => write!(f, "Missing required header: {}", h),
            Error::MalformedHeader(ref h, ref r) => {
                write!(f, "Malformed header: {}: {}", h, r)
            }
            Error::ReadData(_) => write!(f, "Error reading data source."),
            Error::ReadOverflow => write!(f, "Read further than expected."),
            Error::UnexpectedEOB => write!(f, "Unexpected end of body."),
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::ReadData(ref e) => Some(e),
            _ => None,
        }
    }
}
