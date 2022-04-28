#![allow(dead_code)]

use std::{error::Error, net::SocketAddr, time::Duration};

use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Framed};
use tracing::info;

use crate::proto::{ProtoCodec, ProtoValue};

pub async fn run() {
    info!("Starting the coordinator");
    run_coordinator().await.unwrap();
}

async fn run_coordinator() -> Result<(), Box<dyn Error>> {
    let mut participants = vec![Participant {
        id: 1,
        status: Status::Down,
        address: "127.0.0.1:4444".parse().unwrap(),
        conn: None,
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

        let codec = ProtoCodec {};
        let conn = codec.framed(stream.unwrap());
        pa.conn = Some(conn);

        info!("Connected to participant #{}, handshaking...", pa.id);

        let handshake = ProtoValue::Handshake(pa.id);
        pa.send_value(handshake).await;

        let proto_value = pa.receive_value().await;

        match proto_value {
            ProtoValue::Ack(0) => {
                info!("Participant #{} acked 0, launching", pa.id);
                pa.status = Status::Launching;
            }
            ProtoValue::Ack(id) => {
                assert_eq!(pa.id, id);
                pa.status = Status::Launching;
            }
            _ => {
                panic!("Invalid response packet from participant #{}", pa.id);
            }
        }
    }
}

#[derive(Debug)]
enum Status {
    Down,
    Launching,
    Replicating,
}

#[derive(Debug)]
struct Participant {
    id: u32,
    status: Status,
    address: SocketAddr,
    conn: Option<Framed<TcpStream, ProtoCodec>>,
}

impl Participant {
    #[must_use = "future must be used"]
    async fn send_value(&mut self, value: ProtoValue) {
        let conn = self.conn.as_mut().unwrap();
        conn.send(value).await.unwrap(); // FIXME: Handle error
    }

    // FIXME: Return error
    #[must_use = "future must be used"]
    async fn receive_value(&mut self) -> ProtoValue {
        let conn = self.conn.as_mut().unwrap();
        conn.next().await.unwrap().unwrap()
    }
}
