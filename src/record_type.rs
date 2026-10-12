#![allow(missing_docs)]

use std::fmt::Display;
#[derive(Clone, Debug, PartialEq)]
pub enum RecordType {
    WarcInfo,
    Response,
    Resource,
    Request,
    Metadata,
    Revisit,
    Conversion,
    Continuation,
    Unknown(String),
}

const KNOWN_TYPES: [RecordType; 8] = [
    RecordType::WarcInfo,
    RecordType::Response,
    RecordType::Resource,
    RecordType::Request,
    RecordType::Metadata,
    RecordType::Revisit,
    RecordType::Conversion,
    RecordType::Continuation,
];

impl RecordType {
    /// Returns the lowercase record type name.
    pub fn as_str(&self) -> &str {
        match *self {
            RecordType::WarcInfo => "warcinfo",
            RecordType::Response => "response",
            RecordType::Resource => "resource",
            RecordType::Request => "request",
            RecordType::Metadata => "metadata",
            RecordType::Revisit => "revisit",
            RecordType::Conversion => "conversion",
            RecordType::Continuation => "continuation",
            RecordType::Unknown(ref val) => val.as_ref(),
        }
    }
}

impl Display for RecordType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<S: AsRef<str>> From<S> for RecordType {
    fn from(string: S) -> Self {
        let string = string.as_ref();
        KNOWN_TYPES
            .iter()
            .find(|record_type| record_type.as_str().eq_ignore_ascii_case(string))
            .cloned()
            .unwrap_or_else(|| RecordType::Unknown(string.to_lowercase()))
    }
}
