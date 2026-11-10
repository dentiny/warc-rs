#![allow(missing_docs)]

use crate::named_type::named_type;

named_type! {
    RecordType {
        WarcInfo => "warcinfo",
        Response => "response",
        Resource => "resource",
        Request => "request",
        Metadata => "metadata",
        Revisit => "revisit",
        Conversion => "conversion",
        Continuation => "continuation",
    }
}
