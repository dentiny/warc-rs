#![allow(missing_docs)]

use std::fmt::Display;
#[derive(Clone, Debug, PartialEq)]
pub enum TruncatedType {
    Length,
    Time,
    Disconnect,
    Unspecified,
    Unknown(String),
}

const KNOWN_TYPES: [TruncatedType; 4] = [
    TruncatedType::Length,
    TruncatedType::Time,
    TruncatedType::Disconnect,
    TruncatedType::Unspecified,
];

impl TruncatedType {
    /// Returns the lowercase truncation reason.
    pub fn as_str(&self) -> &str {
        match *self {
            TruncatedType::Length => "length",
            TruncatedType::Time => "time",
            TruncatedType::Disconnect => "disconnect",
            TruncatedType::Unspecified => "unspecified",
            TruncatedType::Unknown(ref val) => val.as_ref(),
        }
    }
}

impl Display for TruncatedType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<S: AsRef<str>> From<S> for TruncatedType {
    fn from(string: S) -> Self {
        let string = string.as_ref();
        KNOWN_TYPES
            .iter()
            .find(|truncated_type| truncated_type.as_str().eq_ignore_ascii_case(string))
            .cloned()
            .unwrap_or_else(|| TruncatedType::Unknown(string.to_lowercase()))
    }
}
