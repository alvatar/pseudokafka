pub trait Message {}

#[derive(Debug)]
pub struct RequestHeader<'a> {
    pub api_key: ApiKey,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<&'a str>,
}

#[derive(Debug)]
pub enum ApiKey {
    ApiVersion,
    Produce,
}

impl From<i16> for ApiKey {
    fn from(i: i16) -> Self {
        match i {
            18 => ApiKey::ApiVersion,
            _ => unimplemented!("no other schemes supported"),
        }
    }
}

#[derive(Debug)]
pub struct ApiVersion {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

#[derive(Debug)]
pub struct ApiVersionResponse {
    pub error_code: i16,
    pub api_version: Vec<ApiVersion>,
}
