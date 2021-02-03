use nom::error::{context, VerboseError};
use nom::number::streaming::{be_i16, be_i32};
use nom::sequence::tuple;
use nom::IResult;

use std::io;
use std::io::Read;
use std::mem;
use std::net::TcpStream;
use std::result;

pub use crate::messages::*;

type ParseResult<T, U> = IResult<T, U, VerboseError<T>>;

#[derive(Debug)]
pub struct RawRequest {
    pub size: usize,
    pub body: Vec<u8>,
}

pub fn from_tcp_stream(mut stream: &TcpStream) -> io::Result<RawRequest> {
    let mut size_buf = [0u8; mem::size_of::<i32>()];
    stream.read_exact(&mut size_buf)?;
    let size = i32::from_be_bytes(size_buf) as usize;
    if size > 100_000 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "message too large",
        ));
    }

    let mut body = vec![0u8; size];
    stream.read_exact(&mut body)?;

    Ok(RawRequest { size, body })
}

impl RawRequest {
    pub fn parse(&mut self) -> result::Result<RequestHeader, String> {
        match self.parse_header() {
            Ok(o) => Ok(o.1),
            Err(e) => Err(format!("parse error: {}", e)),
        }

        // NEXT (2) Parse types
    }

    pub fn parse_header(&mut self) -> ParseResult<&[u8], RequestHeader> {
        let parser = tuple((be_i16, be_i16, be_i32));
        context("parse header", parser)(&self.body[..]).map(|(next_input, res)| {
            (
                next_input,
                RequestHeader {
                    api_key: res.0.into(),
                    api_version: res.1,
                    correlation_id: res.2,
                    client_id: None,
                },
            )
        })
    }
}
