use nom::{
    do_parse,
    error::{context, ErrorKind},
    map_res, named,
    number::streaming::{be_i16, be_i32},
    take, tuple, IResult,
};
use num_traits::FromPrimitive;

use std::io::{self, Read};
use std::mem;

pub use crate::messages::*;

const MAX_MESSAGE_SIZE: usize = 100_000;

type NomResult<T, U> = IResult<T, U, nom::error::Error<T>>;

#[derive(Debug)]
pub struct RawRequest {
    pub size: usize,
    pub body: Vec<u8>,
}

pub fn from_stream(mut stream: impl Read) -> io::Result<RequestMessage> {
    let mut size_buf = [0u8; mem::size_of::<i32>()];
    stream.read_exact(&mut size_buf)?;
    let size = i32::from_be_bytes(size_buf) as usize;
    if size > MAX_MESSAGE_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Message too large",
        ));
    }

    let mut contents = vec![0u8; size];
    stream.read_exact(&mut contents)?;

    match parse_header(&*contents) {
        Ok((rest, header)) => match &header.api_key {
            ApiKey::ApiVersions => Ok(ApiVersionsRequest::new_from_bytes(header, rest)),
            ApiKey::Metadata => Ok(MetadataRequest::new_from_bytes(header, rest)),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unknown API key: {:?}", header.api_key),
            )),
        },
        Err(e) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Malformed message: {}", e),
        )),
    }
}

pub fn parse_header(buf: &[u8]) -> NomResult<&[u8], RequestHeader> {
    named!(
        client_id,
        // client_id is a length-prefixed string
        do_parse!(length: be_i16 >> bytes: take!(length) >> (bytes))
    );
    named!(
        header<&[u8], RequestHeader>,
        map_res!(
            // Parse api_key, api_version, correlation_id, client_id
            tuple!(be_i16, be_i16, be_i32, client_id),
            |(api_key_i16, api_version, correlation_id, client_id)| {
                match ApiKey::from_i16(api_key_i16)
                {
                    Some(api_key) => Ok(RequestHeader {
                        api_key,
                        api_version,
                        correlation_id,
                        client_id: Some(std::str::from_utf8(client_id).unwrap().to_string()),
                    }),
                    None => Err(nom::Err::Error((api_key_i16, ErrorKind::Digit))),
                }
            }
        )
    );
    context("parse header", header)(buf)
}

trait Deserialize {
    fn new_from_bytes(header: RequestHeader, bytes: &[u8]) -> RequestMessage;
}

impl Deserialize for ApiVersionsRequest {
    fn new_from_bytes(header: RequestHeader, _bytes: &[u8]) -> RequestMessage {
        // Note: no deserialization is made of the body, since it is unnecessary
        RequestMessage {
            header,
            body: Request::ApiVersionsRequest(ApiVersionsRequest {}),
        }
    }
}

impl Deserialize for MetadataRequest {
    fn new_from_bytes(header: RequestHeader, bytes: &[u8]) -> RequestMessage {
        RequestMessage {
            header,
            body: Request::MetadataRequest(MetadataRequest {}),
        }
    }
}
