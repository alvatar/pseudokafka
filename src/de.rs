use nom::{
    count, do_parse,
    error::{context, ErrorKind},
    map_res, named,
    number::streaming::{be_i16, be_i32, be_u8},
    take, tuple, IResult,
};
use num_traits::FromPrimitive;

use std::io::Read;
use std::mem;

use crate::error::*;
use crate::messages::*;

// TODO: make it configurable replica.fetch.max.bytes
// Default max size
const MAX_MESSAGE_SIZE: usize = 1_048_576;

type NomResult<T, U> = IResult<T, U, nom::error::Error<T>>;

type DeserializeResult = Result<RequestMessage, KafkaError>;

#[derive(Debug)]
pub struct RawRequest {
    pub size: usize,
    pub body: Vec<u8>,
}

/// Deserialize a Kafka message from a stream
///
/// * `stream` - input stream
pub fn from_stream(mut stream: impl Read) -> DeserializeResult {
    let mut size_buf = [0u8; mem::size_of::<i32>()];
    stream.read_exact(&mut size_buf)?;
    let size = i32::from_be_bytes(size_buf) as usize;
    if size > MAX_MESSAGE_SIZE {
        return Err(KafkaError::MessageTooLargeError);
    }

    let mut contents = vec![0u8; size];
    stream.read_exact(&mut contents)?;

    let (rest, header) = parse_header(&*contents)?;
    match &header.api_key {
        ApiKey::ApiVersions => Ok(ApiVersionsRequest::new_from_bytes(rest, header)?),
        ApiKey::Metadata => Ok(MetadataRequest::new_from_bytes(rest, header)?),
        _ => Err(KafkaError::UnknownMessageError(header.api_key)),
    }
}

/// Parse the header of the Kafka messages, so the specific type can be instantiated
/// Returns a NomResult so parsing can continue within each specialized message type.
///
/// * `buf` - input buffer as bytes
pub fn parse_header(buf: &[u8]) -> NomResult<&[u8], RequestHeader> {
    named!(
        client_id,
        // client_id is a length-prefixed string
        do_parse!(length: be_i16 >> bytes: take!(length) >> (bytes))
    );
    named!(
        fixed_size_fields<(i16, i16, i32, &[u8])>,
        // Parse api_key, api_version, correlation_id, client_id
        tuple!(be_i16, be_i16, be_i32, client_id)
    );
    named!(
        header<&[u8], RequestHeader>,
        map_res!(
            fixed_size_fields,
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

//
// Common parsers
//

named!(
    tagged_fields,
    // TODO: implement real support for tagged fields
    // We are currently ignoring these
    do_parse!(length: be_u8 >> bytes: take!(length) >> (bytes))
);

/// Deserialize trait
///
/// All the message body types need to implement this for deserialization
trait Deserialize {
    fn new_from_bytes(buf: &[u8], header: RequestHeader) -> DeserializeResult;
}

impl Deserialize for ApiVersionsRequest {
    fn new_from_bytes(_: &[u8], header: RequestHeader) -> DeserializeResult {
        // Note: no deserialization is made of the body, since it is unnecessary
        Ok(RequestMessage {
            header,
            body: Request::ApiVersionsRequest(ApiVersionsRequest {}),
        })
    }
}

impl Deserialize for MetadataRequest {
    fn new_from_bytes(buf: &[u8], header: RequestHeader) -> DeserializeResult {
        named!(
            topic<MetadataTopicField>,
            do_parse!(
                length: be_u8
                    >> bytes: take!(length)
                    >> (MetadataTopicField {
                        name: std::str::from_utf8(bytes).unwrap().to_string(),
                    })
            )
        );
        named!(
            topics<Vec<MetadataTopicField>>,
            do_parse!(
                num_topics: be_u8
                    >> topics_ls: count!(topic, (num_topics - 1) as usize)
                    >> (topics_ls)
            )
        );
        named!(options<(u8, u8, u8)>, tuple!(be_u8, be_u8, be_u8));
        named!(
            metadata<(&[u8], Vec<MetadataTopicField>, (u8, u8, u8), &[u8])>,
            tuple!(tagged_fields, topics, options, tagged_fields)
        );
        match metadata(buf) {
            Ok((_, (_, topics, (o1, o2, o3), _))) => Ok(RequestMessage {
                header,
                body: Request::MetadataRequest(MetadataRequest {
                    topics,
                    allow_auto_topic_creation: o1 != 0,
                    include_cluster_authorized_operations: o2 != 0,
                    include_topic_authorized_operations: o3 != 0,
                }),
            }),
            Err(_) => Err(KafkaError::DeserializeError),
        }
    }
}
