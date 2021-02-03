use byteorder::{NetworkEndian, WriteBytesExt};
use std::io::Cursor;

pub use crate::messages::*;

type Result<T> = std::io::Result<T>;

pub trait Serialize {
    fn serialize(&self, ser: &mut Serializer) -> Result<()>;
}

pub struct Serializer {
    output: Vec<u8>,
}

pub fn to_bytes<T>(value: &T) -> Result<Vec<u8>>
where
    T: Serialize,
{
    let mut serializer = Serializer {
        output: Vec::<u8>::new(),
    };
    value.serialize(&mut serializer)?;
    Ok(serializer.output)
}

impl Serialize for ApiVersionResponse {
    fn serialize(&self, ser: &mut Serializer) -> Result<()> {
        let mut cursor = Cursor::new(&mut ser.output);
        cursor.write_i16::<NetworkEndian>(self.error_code)?;

        Ok(())
    }
}

#[test]
fn serialize_api_version_response() {
    let msg = ApiVersionResponse {
        error_code: 1,
        api_version: Vec::<ApiVersion>::new(),
    };
    assert_eq!(to_bytes(&msg).unwrap(), vec![0, 1]);
}
