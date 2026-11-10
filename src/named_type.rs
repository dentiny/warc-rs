macro_rules! named_type {
    ($name:ident { $($variant:ident => $wire:literal),+ $(,)? }) => {
        #[derive(Clone, Debug, PartialEq)]
        pub enum $name {
            $($variant,)+
            Unknown(String),
        }

        const KNOWN_TYPES: &[$name] = &[$($name::$variant,)+];

        impl $name {
            /// Returns the canonical wire value, or the retained unknown value.
            pub fn as_str(&self) -> &str {
                match self {
                    $(Self::$variant => $wire,)+
                    Self::Unknown(value) => value,
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl<S: AsRef<str>> From<S> for $name {
            fn from(value: S) -> Self {
                let value = value.as_ref();
                KNOWN_TYPES
                    .iter()
                    .find(|known| known.as_str().eq_ignore_ascii_case(value))
                    .cloned()
                    .unwrap_or_else(|| Self::Unknown(value.to_lowercase()))
            }
        }
    };
}

pub(crate) use named_type;
