use std::net::TcpListener;
use std::net::TcpStream;
use std::thread;

mod de;
mod ser;
mod messages;

pub use crate::de::*;
pub use crate::ser::*;
pub use crate::messages::*;

const KAFKA_HOST: &str = "0.0.0.0:9093";

fn handle_client(stream: TcpStream) {
    if let Ok(mut req) = de::from_tcp_stream(&stream) {
        if let Ok(req) = req.parse() {
            match req.api_key {
                ApiKey::ApiVersion => unimplemented!("HERE ------- Serialize Response"),
                _ => unimplemented!(),
            }
        }
        println!("request: {:?}", req);
    } else {
        println!("error, TODO")
    }
}

fn main() {
    let listener = TcpListener::bind(KAFKA_HOST).unwrap();
    // accept connections and process them, spawning a new thread for each one
    println!("Server listening on {}", KAFKA_HOST);
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                // println!("New connection: {}", stream.peer_addr().unwrap());
                thread::spawn(move || handle_client(stream));
            }
            Err(e) => {
                println!("Error: {}", e);
            }
        }
    }
}
