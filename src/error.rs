use nom::{Err,error};

use crate::messages::ApiKey;

#[derive(Debug)]
pub enum KafkaError {
    SerializeError,
    DeserializeError,
    MessageTooLargeError,
    UnknownMessageError(ApiKey),
}

impl From<std::io::Error> for KafkaError {
    fn from (_: std::io::Error) -> Self {
        KafkaError::SerializeError
    }
}

impl From<Err<error::Error<&[u8]>>> for KafkaError {
    fn from (_: Err<error::Error<&[u8]>>) -> Self {
        KafkaError::DeserializeError
    }
}
