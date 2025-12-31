#![allow(unused_imports)]
use anyhow::Result;
use std::{
    io::Write,
    net::{TcpListener, TcpStream},
};

fn main() -> Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    let server = KafkaServer::new();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                server.process(&mut stream)?;
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    Ok(())
}

struct KafkaServer {}

impl KafkaServer {
    pub fn new() -> Self {
        Self {}
    }

    pub fn process(&self, stream: &mut TcpStream) -> Result<()> {
        let message_size: u32 = 0;
        let message_size_payload = message_size.to_be_bytes();

        let correlation_id: i32 = 7;
        let correlation_id_payload = correlation_id.to_be_bytes();

        stream.write(&message_size_payload)?;
        stream.write(&correlation_id_payload)?;

        Ok(())
    }
}
