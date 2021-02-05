use nom::combinator::map_res;
use nom::error::{context, ErrorKind, VerboseError};
use nom::number::streaming::{be_i16, be_i32};
use nom::sequence::tuple;
use nom::IResult;
use num_traits::FromPrimitive;

use std::io;
use std::io::Read;
use std::mem;

pub use crate::messages::*;

const MAX_MESSAGE_SIZE: usize = 100_000;

type NomResult<T, U> = IResult<T, U, VerboseError<T>>;

#[derive(Debug)]
pub struct RawRequest {
    pub size: usize,
    pub body: Vec<u8>,
}

pub fn from_tcp_stream(mut stream: impl Read) -> io::Result<RequestMessage> {
    let mut size_buf = [0u8; mem::size_of::<i32>()];
    stream.read_exact(&mut size_buf)?;
    let size = i32::from_be_bytes(size_buf) as usize;
    if size > MAX_MESSAGE_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "message too large",
        ));
    }

    let mut contents = vec![0u8; size];
    stream.read_exact(&mut contents)?;

    match parse_header(&*contents) {
        Ok((rest, header)) => match &header.api_key {
            ApiKey::ApiVersions => Ok(RequestMessage::new_from_bytes(header, rest)),
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
    let parser = map_res(
        tuple((be_i16, be_i16, be_i32)),
        |(api_key_i16, api_version, correlation_id)| match ApiKey::from_i16(api_key_i16) {
            Some(api_key) => Ok(RequestHeader {
                api_key,
                api_version,
                correlation_id,
                client_id: None,
            }),
            None => Err(nom::Err::Error((&buf[..2], ErrorKind::Digit))),
        },
    );
    context("parse header", parser)(&buf[..])
}

impl RequestMessage {
    fn new_from_bytes(header: RequestHeader, bytes: &[u8]) -> Self {
        Self{
            header,
            body: Request::ApiVersionsRequest(ApiVersionsRequest {}),
        }
    }
}
