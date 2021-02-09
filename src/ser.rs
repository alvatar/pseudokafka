use byteorder::{NetworkEndian, WriteBytesExt};

use std::io::{Cursor, Write};
use std::mem;

use crate::error::*;
use crate::messages::*;

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
        let mut cursor = Cursor::new(Vec::<u8>::new());

        /*
        let api_version_length = mem::size_of::<i16>() * 3 + mem::size_of::<u8>();
        let msg_length = (mem::size_of::<u32>() * 2
            + mem::size_of::<i16>()
            + mem::size_of::<u8>() * 2
            + self.api_versions.len() * api_version_length) as u32;
         */

        cursor.write_u32::<NetworkEndian>(0)?;
        cursor.write_u32::<NetworkEndian>(self.header.correlation_id)?;
        cursor.write_u16::<NetworkEndian>(self.error_code)?;

        cursor.write_u8(self.api_versions.len() as u8 + 1)?;
        for version in &self.api_versions {
            cursor.write_u16::<NetworkEndian>(version.api_key)?;
            cursor.write_u16::<NetworkEndian>(version.min_version)?;
            cursor.write_u16::<NetworkEndian>(version.max_version)?;
            cursor.write_u8(0)?;
        }

        cursor.write_u32::<NetworkEndian>(self.throttle_time)?;
        // Tagged fields (none)
        cursor.write_u8(0)?;

        // Write length at the beginning
        let msg_length = cursor.position();
        cursor.set_position(0);
        cursor.write_u32::<NetworkEndian>((msg_length - mem::size_of::<u32>() as u64) as u32)?;

        Ok(cursor.into_inner())
    }
}

impl Serialize for MetadataResponse {
    fn to_bytes(&self) -> SerializeResult {
        let mut cursor = Cursor::new(Vec::<u8>::new());

        cursor.write_u32::<NetworkEndian>(0)?;
        cursor.write_u32::<NetworkEndian>(self.header.correlation_id)?;
        // Tagged fields (none)
        cursor.write_u8(0)?;
        cursor.write_u32::<NetworkEndian>(self.throttle_time)?;

        cursor.write_u8(self.brokers.len() as u8 + 1)?;
        for broker in &self.brokers {
            cursor.write_u32::<NetworkEndian>(broker.node_id)?;
            cursor.write_u8(broker.host.len() as u8 + 1)?;
            cursor.write(broker.host.as_bytes())?;
            cursor.write_u32::<NetworkEndian>(broker.port)?;
            // Rack (none)
            cursor.write_u8(0)?;
            // Tagged fields (none)
            cursor.write_u8(0)?;
        }

        cursor.write_u8(self.cluster_id.len() as u8 + 1)?;
        cursor.write(self.cluster_id.as_bytes())?;
        cursor.write_u32::<NetworkEndian>(self.controller_id)?;

        cursor.write_u8(self.topics.len() as u8 + 1)?;
        for topic in &self.topics {
            cursor.write_u16::<NetworkEndian>(topic.error)?;
            cursor.write_u8(topic.name.len() as u8 + 1)?;
            cursor.write(topic.name.as_bytes())?;
            cursor.write_u8(topic.is_internal as u8)?;
            // Partitions
            cursor.write_u8(topic.partitions.len() as u8 + 1)?;
            for partition in &topic.partitions {
                cursor.write_u16::<NetworkEndian>(partition.error)?;
                cursor.write_u32::<NetworkEndian>(partition.id)?;
                cursor.write_u32::<NetworkEndian>(partition.leader_id)?;
                cursor.write_u32::<NetworkEndian>(partition.leader_epoch)?;
                cursor.write_u8(partition.replicas.len() as u8 + 1)?;
                for replica in &partition.replicas {
                    cursor.write_u32::<NetworkEndian>(*replica)?;
                }

                // Tagged fields (none)
                cursor.write_u8(0)?;
            }
        }

        cursor.write_u32::<NetworkEndian>(self.cluster_authorized_operations)?;
        // Tagged fields (none)
        cursor.write_u8(0)?;

        // Write length at the beginning
        let msg_length = cursor.position();
        cursor.set_position(0);
        cursor.write_u32::<NetworkEndian>((msg_length - mem::size_of::<u32>() as u64) as u32)?;

        Ok(cursor.into_inner())
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
            topics: Vec::<MetadataTopic>::new(),
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
}
