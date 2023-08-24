use std::{fmt::Display, self};
use serde::{de, ser};

/// Generic Error type.
#[derive(Debug)]
pub struct Error {
    /// Error message.
    pub message: String
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Error: {}", self.message)
    }
}

impl std::error::Error for Error {

}

impl de::Error for Error {
    fn custom<T>(msg:T) -> Self where T:Display {
        Error { message: format!("{}", msg) }
    }
}

impl ser::Error for Error {
    fn custom<T>(msg:T) -> Self where T:Display {
        Error {message: format!("{}", msg)}
    }
}
