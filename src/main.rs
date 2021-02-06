use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;
use std::thread;

mod de;
mod messages;
mod ser;

pub use crate::de::*;
pub use crate::messages::*;
pub use crate::ser::*;

const KAFKA_HOST: &str = "0.0.0.0:9093";

fn handle_client(mut stream: TcpStream) {
    loop {
        match de::from_stream(&stream) {
            Ok(req) => {
                match response_for(&req) {
                    Some(resp) => {
                        stream.write(resp.to_bytes().unwrap().as_slice()).unwrap();
                    }
                    None => break,
                }
            }
            Err(e) => {
                println!("Error reading message: {}", e.into_inner().unwrap());
                break;
            }
        }
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
