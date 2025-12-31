#![allow(unused_imports)]
use anyhow::Result;
use std::sync::{Arc, atomic::AtomicU64, atomic::Ordering::Relaxed};
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
    let mut task_counter: u64 = 0;
    let concurrency = Arc::new(AtomicU64::new(0));

    loop {
        let (mut stream, _addr) = listener.accept().await?;
        task_counter += 1;

        let task_id = task_counter;
        let mut cp = server.processor(task_id, concurrency.clone()).await?;
        tokio::spawn(async move {
            if let Err(e) = cp.process(&mut stream).await {
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

    pub async fn processor(
        &self,
        id: u64,
        concurrency_count: Arc<AtomicU64>,
    ) -> Result<ClientProcessor> {
        Ok(ClientProcessor::new(id, concurrency_count).await)
    }
}

struct ClientProcessor {
    id: u64,
    concurrency_count: Arc<AtomicU64>,
}

impl ClientProcessor {
    pub async fn new(id: u64, concurrency_count: Arc<AtomicU64>) -> Self {
        Self {
            id,
            concurrency_count,
        }
    }

    pub async fn process(&mut self, stream: &mut TcpStream) -> Result<()> {
        let concur = self.concurrency_count.fetch_add(1, Relaxed);
        println!(
            "CP {} started. Concurrency {} -> {}.",
            self.id,
            concur,
            concur + 1
        );

        let message_size: u32 = 0;
        let message_size_payload = message_size.to_be_bytes();

        let correlation_id: i32 = 7;
        let correlation_id_payload = correlation_id.to_be_bytes();

        stream.write_all(&message_size_payload).await?;
        stream.write_all(&correlation_id_payload).await?;

        let concur = self.concurrency_count.fetch_sub(1, Relaxed);
        println!(
            "CP {} completed. Concurrency {} -> {}",
            self.id,
            concur,
            concur - 1
        );
        Ok(())
    }
}
