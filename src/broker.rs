use crate::messages::*;

pub fn process(req: &Request) -> Option<Response> {
    match &req {
        Request::ApiVersionsRequest(req) => Some(Response::ApiVersionsResponse(
            ApiVersionsResponse::new(&req),
        )),
        Request::MetadataRequest(req) => {
            Some(Response::MetadataResponse(MetadataResponse::new(&req)))
        }
    }
}
