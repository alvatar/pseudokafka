use nom::{
    count, do_parse,
    error::{context, ErrorKind},
    map_res, named,
    number::streaming::{be_i16, be_i32, be_i64, be_u16, be_u32, be_u64, be_u8},
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

type DeserializeResult = Result<Request, KafkaError>;

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
        ApiKey::Produce => Ok(ProduceRequest::new_from_bytes(rest, header)?),
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
        do_parse!(length: be_u16 >> bytes: take!(length) >> (bytes))
    );
    named!(
        fixed_size_fields<(u16, u16, u32, &[u8])>,
        // Parse api_key, api_version, correlation_id, client_id
        tuple!(be_u16, be_u16, be_u32, client_id)
    );
    named!(
        header<&[u8], RequestHeader>,
        map_res!(
            fixed_size_fields,
            |(api_key_u16, api_version, correlation_id, client_id)| {
                match ApiKey::from_u16(api_key_u16)
                {
                    Some(api_key) => Ok(RequestHeader {
                        api_key,
                        api_version,
                        correlation_id,
                        client_id: Some(std::str::from_utf8(client_id).unwrap().to_string()),
                    }),
                    None => Err(nom::Err::Error((api_key_u16, ErrorKind::Digit))),
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
        Ok(Request::ApiVersionsRequest(ApiVersionsRequest { header }))
    }
}

impl Deserialize for MetadataRequest {
    fn new_from_bytes(buf: &[u8], header: RequestHeader) -> DeserializeResult {
        named!(
            topic<String>,
            do_parse!(
                length: be_u8
                >> bytes: take!(length-1)
                >> ignore: take!(1) // it seems Kakfa uses also a null-ending
                >> (std::str::from_utf8(bytes).unwrap().to_string())
            )
        );
        named!(
            topics<Vec<String>>,
            do_parse!(
                num_topics: be_u8
                    >> topics_ls: count!(topic, (num_topics - 1) as usize)
                    >> (topics_ls)
            )
        );
        named!(options<(u8, u8, u8)>, tuple!(be_u8, be_u8, be_u8));
        named!(
            metadata<(&[u8], Vec<String>, (u8, u8, u8), &[u8])>,
            tuple!(tagged_fields, topics, options, tagged_fields)
        );
        match metadata(buf) {
            Ok((_, (_, topics, (o1, o2, o3), _))) => {
                Ok(Request::MetadataRequest(MetadataRequest {
                    header,
                    topics,
                    allow_auto_topic_creation: o1 != 0,
                    include_cluster_authorized_operations: o2 != 0,
                    include_topic_authorized_operations: o3 != 0,
                }))
            }
            Err(_) => Err(KafkaError::DeserializeError),
        }
    }
}

impl Deserialize for ProduceRequest {
    fn new_from_bytes(buf: &[u8], header: RequestHeader) -> DeserializeResult {
        named!(
            record<ProduceRecordRequest>,
            do_parse!(
                unknown: be_u8
                    >> record_attributes: be_u8
                    >> timestamp: be_u8 // this u8 is according to wireshark a timestamp (?)
                    >> offset: be_u8
                    >> unknown2: be_u8 // TODO
                    >> unknown3: be_u8 // TODO
                    >> value: take!(1)
                    >> headers: be_u8 // TODO
                    >> (ProduceRecordRequest {
                        value: value.to_vec()
                    }))
        );
        named!(
            partition<ProducePartitionRequest>,
            do_parse!(
                partition_id: be_u32
                    >> num_bytes: be_u32 // ignored
                    >> offset: be_u64
                    >> msg_size: be_u32 // ignored
                    >> leader_epoch: be_i32
                    >> magic_byte: be_u8 // ignored
                    >> crc32: be_u32 // ignored
                    >> options: be_u16
                    >> last_offset_delta: be_u32
                    >> first_timestamp: be_u64
                    >> last_timestamp: be_u64
                    >> producer_id: be_i64
                    >> producer_epoch: be_i16
                    >> base_sequence: be_i32
                    >> size: be_u32
                    >> records: count!(record, size as usize)
                    >> (ProducePartitionRequest {
                        id: partition_id,
                        message_set: ProduceRecordBatchRequest {
                            offset,
                            leader_epoch,
                            options,
                            last_offset_delta,
                            first_timestamp,
                            last_timestamp,
                            producer_id,
                            producer_epoch,
                            base_sequence,
                            records,
                        }
                    })
            )
        );
        named!(
            topic<ProduceTopicRequest>,
            do_parse!(
                name_length: be_u16
                    >> name_bytes: take!(name_length)
                    >> num_partitions: be_u32
                    >> partitions: count!(partition, num_partitions as usize)
                    >> (ProduceTopicRequest {
                        name: std::str::from_utf8(name_bytes).unwrap().to_string(),
                        partitions: partitions,
                    })
            )
        );
        named!(
            topics<Vec<ProduceTopicRequest>>,
            do_parse!(n: be_u32 >> topics: count!(topic, n as usize) >> (topics))
        );
        named!(
            produce_header<(i16, u16, u32)>,
            tuple!(be_i16, be_u16, be_u32)
        );
        named!(
            produce_request<((i16, u16, u32), Vec<ProduceTopicRequest>)>,
            tuple!(produce_header, topics)
        );
        match produce_request(buf) {
            Ok((_, ((tx_id, req_acks, tout), topics))) => {
                Ok(Request::ProduceRequest(ProduceRequest {
                    header,
                    transactional_id: tx_id,
                    required_acks: req_acks,
                    timeout: tout,
                    topics: topics,
                }))
            }
            Err(_) => Err(KafkaError::DeserializeError),
        }
    }
}

// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_produce_request() {
        let bytes = include_bytes!("../res/produce_request.bin");
        if let Request::ProduceRequest(r) = from_stream(&bytes[..]).unwrap() {
            dbg!(&r);
            assert_eq!(
                r,
                ProduceRequest {
                    header: RequestHeader {
                        api_key: ApiKey::Produce,
                        api_version: 8,
                        correlation_id: 4,
                        client_id: Some("console-producer".to_string()),
                    },
                    transactional_id: -1,
                    required_acks: 1,
                    timeout: 1500,
                    topics: vec![ProduceTopicRequest {
                        name: "my-topic".to_string(),
                        partitions: vec![ProducePartitionRequest {
                            id: 0,
                            message_set: ProduceRecordBatchRequest {
                                offset: 0,
                                leader_epoch: -1,
                                options: 0,
                                last_offset_delta: 0,
                                first_timestamp: 1612447466097,
                                last_timestamp: 1612447466097,
                                producer_id: -1,
                                producer_epoch: -1,
                                base_sequence: -1,
                                records: vec![
                                    ProduceRecordRequest{
                                        value: vec!['a' as u8]
                                    }
                                ],
                            },
                        }]
                    }],
                }
            );
        }
    }
}
