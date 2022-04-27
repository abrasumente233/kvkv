#![allow(dead_code)]

use std::{error::Error, net::SocketAddr, time::Duration};

use tokio::net::TcpStream;
use tracing::info;

pub async fn run() {
    info!("Starting the coordinator");
    run_coordinator().await.unwrap();
}

async fn run_coordinator() -> Result<(), Box<dyn Error>> {
    let mut participants = vec![Participant {
        id: 1,
        status: Status::Launching,
        address: "127.0.0.1:4444".parse().unwrap(),
        stream: None,
    }];

    establish_connection(&mut participants).await;

    Ok(())
}

async fn establish_connection(participants: &mut Vec<Participant>) {
    for pa in participants.iter_mut() {
        let mut stream;
        loop {
            info!("Waiting for participant #{}...", pa.id);
            stream = TcpStream::connect(pa.address).await;
            match stream {
                Ok(_) => break,
                Err(_) => tokio::time::sleep(Duration::from_secs(5)).await,
            };
        }

        pa.stream = Some(stream.unwrap());
        info!("Connected to participant #{}", pa.id);
    }
}

#[derive(Debug)]
enum Status {
    Launching,
    Running,
    Replicating,
}

#[derive(Debug)]
struct Participant {
    id: u32,
    status: Status,
    address: SocketAddr,
    stream: Option<TcpStream>,
}
