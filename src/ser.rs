use byteorder::{NetworkEndian, WriteBytesExt};

use std::io::Cursor;
use std::mem;

use crate::messages::*;

type SerializeResult = std::io::Result<Vec<u8>>;

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

        let header_length = mem::size_of::<i16>() * 3 + mem::size_of::<u8>();
        let self_length = (mem::size_of::<i32>() * 2
            + mem::size_of::<i16>()
            + mem::size_of::<u8>() * 2
            + self.api_versions.len() * header_length) as i32;

        cursor.write_i32::<NetworkEndian>(self_length)?;
        cursor.write_i32::<NetworkEndian>(self.header.correlation_id)?;
        cursor.write_i16::<NetworkEndian>(self.error_code)?;

        // TODO: This is reverse-engineered, and so I can't be 100% certain about the +1
        // Look into Kafka's source code. Perhaps refers to the throttle_time field placed at the very end.
        cursor.write_u8(self.api_versions.len() as u8 + 1)?;

        for version in &self.api_versions {
            cursor.write_i16::<NetworkEndian>(version.api_key)?;
            cursor.write_i16::<NetworkEndian>(version.min_version)?;
            cursor.write_i16::<NetworkEndian>(version.max_version)?;
            cursor.write_u8(0)?;
        }

        cursor.write_i32::<NetworkEndian>(self.throttle_time)?;
        // Tagged fields (none)
        cursor.write_u8(0)?;

        Ok(cursor.into_inner())
    }
}

impl Serialize for MetadataResponse {
    fn to_bytes(&self) -> SerializeResult {
        Ok(Vec::<u8>::new()) // TODO
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
                    api_key: ToPrimitive::to_i16(&ApiKey::Produce).unwrap(),
                    min_version: 0,
                    max_version: 8,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::Fetch).unwrap(),
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
        let req = &RequestMessage {
            header: RequestHeader {
                api_key: ApiKey::Produce,
                api_version: 0,
                correlation_id: 0,
                client_id: None,
            },
            body: Request::ApiVersionsRequest(ApiVersionsRequest {}),
        };
        let msg = ApiVersionsResponse::new(req);
        assert_eq!(
            msg.to_bytes().unwrap(),
            include_bytes!("../res/api_versions_response.bin")
        );
    }
}
