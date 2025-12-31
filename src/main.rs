#![allow(unused_imports)]
use anyhow::Result;
use std::sync::Arc;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() -> Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").await?;

    let server = Arc::new(KafkaServer::new());

    loop {
        let (mut stream, _addr) = listener.accept().await?;
        let server = Arc::clone(&server);
        tokio::spawn(async move {
            if let Err(e) = server.process(&mut stream).await {
                println!("error: {}", e);
            }
        });
    }
}

struct KafkaServer {}

impl KafkaServer {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn process(&self, stream: &mut TcpStream) -> Result<()> {
        let message_size: u32 = 0;
        let message_size_payload = message_size.to_be_bytes();

        let correlation_id: i32 = 7;
        let correlation_id_payload = correlation_id.to_be_bytes();

        stream.write_all(&message_size_payload).await?;
        stream.write_all(&correlation_id_payload).await?;

        Ok(())
    }
}
