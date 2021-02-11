use byteorder::{NetworkEndian, WriteBytesExt};

use std::io::{Cursor, Write};
use std::mem;

use crate::error::*;
use crate::messages::*;

macro_rules! encode_with {
    {
        $cursor:ident:
        $($name:expr),*
    } => {
        $(
            $name.encode($cursor)?;
        )*
    }
}

trait SerializeCursor {
    fn encode(&self, cursor: &mut Cursor<Vec<u8>>) -> std::io::Result<()>;
}

impl SerializeCursor for bool {
    fn encode(&self, cursor: &mut Cursor<Vec<u8>>) -> std::io::Result<()> {
        cursor.write_u8(*self as u8)
    }
}

impl SerializeCursor for u8 {
    fn encode(&self, cursor: &mut Cursor<Vec<u8>>) -> std::io::Result<()> {
        cursor.write_u8(*self)
    }
}

impl SerializeCursor for u16 {
    fn encode(&self, cursor: &mut Cursor<Vec<u8>>) -> std::io::Result<()> {
        cursor.write_u16::<NetworkEndian>(*self)
    }
}

impl SerializeCursor for u32 {
    fn encode(&self, cursor: &mut Cursor<Vec<u8>>) -> std::io::Result<()> {
        cursor.write_u32::<NetworkEndian>(*self)
    }
}

impl SerializeCursor for String {
    fn encode(&self, cursor: &mut Cursor<Vec<u8>>) -> std::io::Result<()> {
        cursor.write_u8(self.len() as u8 + 1)?;
        cursor.write(self.as_bytes())?;
        Ok(())
    }
}

impl<T: SerializeCursor> SerializeCursor for Vec<T> {
    fn encode(&self, cursor: &mut Cursor<Vec<u8>>) -> std::io::Result<()> {
        cursor.write_u8(self.len() as u8 + 1)?;
        for e in self {
            e.encode(cursor)?;
        }
        Ok(())
    }
}

impl SerializeCursor for ApiVersion {
    fn encode(&self, cursor: &mut Cursor<Vec<u8>>) -> std::io::Result<()> {
        encode_with! {
            cursor:
            self.api_key,
            self.min_version,
            self.max_version,
            0u8
        }
        Ok(())
    }
}

impl SerializeCursor for BrokerMetadata {
    fn encode(&self, cursor: &mut Cursor<Vec<u8>>) -> std::io::Result<()> {
        encode_with! {
            cursor:
            self.node_id,
            self.host,
            self.port,
            0u8, // Rack (none)
            0u8 // Tagged fields (none)
        }
        Ok(())
    }
}

impl SerializeCursor for PartitionMetadata {
    fn encode(&self, cursor: &mut Cursor<Vec<u8>>) -> std::io::Result<()> {
        encode_with! {
            cursor:
            self.error,
            self.id,
            self.leader_id,
            self.leader_epoch,
            self.replicas,
            self.caught_up_replicas,
            self.offline_replicas,
            0u8 // Tagged fields (none)
        }
        Ok(())
    }
}

impl SerializeCursor for TopicMetadata {
    fn encode(&self, cursor: &mut Cursor<Vec<u8>>) -> std::io::Result<()> {
        encode_with! {
            cursor:
            self.error,
            self.name,
            self.is_internal,
            self.partitions,
            self.topic_authorized_operations,
            0u8 // Tagged fields (none)
        }
        Ok(())
    }
}

fn write_msg_length(cursor: &mut Cursor<Vec<u8>>) -> std::io::Result<()> {
    let msg_length = cursor.position();
    cursor.set_position(0);
    cursor.write_u32::<NetworkEndian>((msg_length - mem::size_of::<u32>() as u64) as u32)?;
    Ok(())
}

// -----------------------------------------------------------------------------

type SerializeResult = Result<Vec<u8>, KafkaError>;

pub trait Serialize {
    fn to_bytes(&self) -> SerializeResult;
}

impl Serialize for Response {
    fn to_bytes(&self) -> SerializeResult {
        match self {
            Response::ApiVersionsResponse(msg) => msg.to_bytes(),
            Response::MetadataResponse(msg) => msg.to_bytes(),
        }
    }
}

impl Serialize for ApiVersionsResponse {
    fn to_bytes(&self) -> SerializeResult {
        let cursor = &mut Cursor::new(Vec::<u8>::new());
        encode_with! {
            cursor:
            0u32, // Length
            self.header.correlation_id,
            self.error_code,
            &self.api_versions,
            self.throttle_time,
            0u8 // Tagged fields (none)
        }
        write_msg_length(cursor)?;

        Ok(cursor.to_owned().into_inner())
    }
}

impl Serialize for MetadataResponse {
    fn to_bytes(&self) -> SerializeResult {
        let cursor = &mut Cursor::new(Vec::<u8>::new());
        encode_with! {
            cursor:
            0u32, // Length
            self.header.correlation_id,
            0u8, // Tagged fields (none)
            self.throttle_time,
            self.brokers,
            self.cluster_id,
            self.controller_id,
            &self.topics,
            self.cluster_authorized_operations,
            0u8 // Tagged fields (none)
        }
        write_msg_length(cursor)?;

        Ok(cursor.to_owned().into_inner())
    }
}

#[cfg(test)]
mod tests {
    use num_traits::ToPrimitive;

    use super::*;

    #[test]
    fn serialize_api_version_response() {
        let msg = ApiVersionsResponse {
            header: ResponseHeader { correlation_id: 0 },
            error_code: 1,
            throttle_time: 0,
            api_versions: vec![
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::Produce).unwrap(),
                    min_version: 0,
                    max_version: 8,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::Fetch).unwrap(),
                    min_version: 0,
                    max_version: 11,
                },
            ],
        };
        assert_eq!(
            msg.to_bytes().unwrap(),
            vec![
                0, 0, 0, 26, 0, 0, 0, 0, 0, 1, 3, 0, 0, 0, 0, 0, 8, 0, 0, 1, 0, 0, 0, 11, 0, 0, 0,
                0, 0, 0
            ]
        );
    }

    #[test]
    fn serialize_full_api_version_response() {
        let req = &ApiVersionsRequest {
            header: RequestHeader {
                api_key: ApiKey::Produce,
                api_version: 0,
                correlation_id: 0,
                client_id: None,
            },
        };
        let msg = ApiVersionsResponse::new(req);
        assert_eq!(
            msg.to_bytes().unwrap(),
            include_bytes!("../res/api_versions_response.bin")
        );
    }

    #[test]
    fn serialize_no_topics_metadata_response() {
        let req = &MetadataRequest {
            header: RequestHeader {
                api_key: ApiKey::Produce,
                api_version: 9,
                correlation_id: 1,
                client_id: None,
            },
            topics: Vec::<TopicMetadataRequest>::new(),
            allow_auto_topic_creation: true,
            include_cluster_authorized_operations: false,
            include_topic_authorized_operations: false,
        };
        let msg = MetadataResponse::new(req);
        assert_eq!(
            msg.to_bytes().unwrap(),
            include_bytes!("../res/metadata_no_topics_response.bin")
        );
    }

    #[test]
    fn serialize_metadata_response() {
        let req = &MetadataRequest {
            header: RequestHeader {
                api_key: ApiKey::Produce,
                api_version: 9,
                correlation_id: 3,
                client_id: None,
            },
            topics: vec![TopicMetadataRequest {
                name: "my-topic".to_string(),
            }],
            allow_auto_topic_creation: true,
            include_cluster_authorized_operations: false,
            include_topic_authorized_operations: false,
        };
        let msg = MetadataResponse::new(req);
        assert_eq!(
            msg.to_bytes().unwrap(),
            include_bytes!("../res/metadata_response.bin")
        );
    }
}
