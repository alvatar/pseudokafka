use crate::messages::*;

pub fn process(req: &Request) -> Option<Response> {
    match &req {
        Request::ApiVersionsRequest(req) => Some(Response::ApiVersionsResponse(
            ApiVersionsResponse::new(&req),
        )),
        Request::MetadataRequest(req) => {
            Some(Response::MetadataResponse(MetadataResponse::new(&req)))
        }
        Request::ProduceRequest(req) => {
            // TODO: store message production and deliver to subscribers
            Some(Response::ProduceResponse(ProduceResponse::new(&req)))
        }
    }
}
