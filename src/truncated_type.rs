#![allow(missing_docs)]

use crate::named_type::named_type;

named_type! {
    TruncatedType {
        Length => "length",
        Time => "time",
        Disconnect => "disconnect",
        Unspecified => "unspecified",
    }
}
