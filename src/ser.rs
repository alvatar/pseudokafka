use byteorder::{NetworkEndian, WriteBytesExt};

use std::io::{Cursor, Error, ErrorKind};
use std::mem;

use crate::messages::*;

type SerializeResult = std::io::Result<Vec<u8>>;

pub trait Serialize {
    fn to_bytes(&self) -> SerializeResult;
}

impl Serialize for Response {
    fn to_bytes(&self) -> SerializeResult {
        match self {
            Response::ApiVersionsResponse(msg) => to_bytes(msg),
            _ => Err(Error::new(ErrorKind::Other, "oh no!")),
        }
    }
}

fn to_bytes(msg: &ApiVersionsResponse) -> SerializeResult {
    let mut cursor = Cursor::new(Vec::<u8>::new());

    let header_length = mem::size_of::<i16>() * 3 + mem::size_of::<u8>();
    let msg_length = (mem::size_of::<i32>() * 2
        + mem::size_of::<i16>()
        + mem::size_of::<u8>() * 2
        + msg.api_versions.len() * header_length) as i32;

    cursor.write_i32::<NetworkEndian>(msg_length)?;
    cursor.write_i32::<NetworkEndian>(msg.header.correlation_id)?;
    cursor.write_i16::<NetworkEndian>(msg.error_code)?;

    // TODO: This is reverse-engineered, and so I can't be 100% certain about the +1
    // Look into Kafka's source code. Perhaps refers to the throttle_time field placed at the very end.
    cursor.write_u8(msg.api_versions.len() as u8 + 1)?;

    for version in &msg.api_versions {
        cursor.write_i16::<NetworkEndian>(version.api_key)?;
        cursor.write_i16::<NetworkEndian>(version.min_version)?;
        cursor.write_i16::<NetworkEndian>(version.max_version)?;
        cursor.write_u8(0)?;
    }

    cursor.write_i32::<NetworkEndian>(msg.throttle_time)?;
    // Tagged fields (none)
    cursor.write_u8(0)?;

    Ok(cursor.into_inner())
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
            to_bytes(&msg).unwrap(),
            vec![
                0, 0, 0, 26, 0, 0, 0, 0, 0, 1, 3, 0, 0, 0, 0, 0, 8, 0, 0, 1, 0, 0, 0, 11, 0, 0, 0,
                0, 0, 0
            ]
        );
    }

    #[test]
    fn serialize_full_api_version_response() {
        let hdr = &RequestHeader{
            api_key: ApiKey::Produce,
            api_version: 0,
            correlation_id: 0,
            client_id: None,
        };
        let msg = ApiVersionsResponse::new(hdr);
        assert_eq!(
            to_bytes(&msg).unwrap(),
            include_bytes!("../res/api_versions_response.bin")
        );
    }
}
