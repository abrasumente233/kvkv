#![allow(dead_code)]

use std::{error::Error, net::SocketAddr, time::Duration};

use futures::{SinkExt, StreamExt};
use tokio::{
    join,
    net::{TcpListener, TcpStream},
    spawn,
    sync::{mpsc, oneshot},
};
use tokio_util::codec::{Decoder, Framed};
use tracing::{info, trace};

use crate::{
    proto::{ProtoCodec, ProtoValue},
    resp::{RespCodec, RespValue},
};

type CordMessage = (RespValue, oneshot::Sender<RespValue>);

pub async fn run(port: u16) -> Result<(), Box<dyn Error>> {
    let (tx_resp, rx_resp) = mpsc::channel::<CordMessage>(16);

    let values = join!(
        spawn(async move { Coordinator::new(rx_resp).run().await.unwrap() }),
        spawn(async move { listen_for_clients(tx_resp, port).await.unwrap() })
    );

    (values.0?, values.1?);
    Ok(())
}

async fn listen_for_clients(
    tx_resp: mpsc::Sender<CordMessage>,
    port: u16,
) -> Result<(), Box<dyn Error>> {
    let address = format!("127.0.0.1:{}", port);
    info!("listening on {}", address);
    let listener = TcpListener::bind(address).await?;

    loop {
        let tx_resp = tx_resp.clone();
        let (socket, _) = listener.accept().await?;
        handle_client_socket(socket, tx_resp).await;
    }
}

async fn handle_client_socket(socket: TcpStream, tx_resp: mpsc::Sender<CordMessage>) {
    // NOTE: The coordinator must decode the packet in order to know
    // if there's any valid RESP packet to be forward.
    // Seems wasteful.
    let codec = RespCodec {};
    let mut conn = codec.framed(socket);
    while let Some(message) = conn.next().await {
        if let Ok(resp_value) = message {
            let (tx, rx) = oneshot::channel();
            // We just unwrap because we don't have much to do
            // if inter task communication failed...
            tx_resp.send((resp_value, tx)).await.unwrap();

            match rx.await {
                Ok(value) => conn.send(value).await.unwrap(), // FIXME: handle connection error
                Err(_) => panic!("coordinator returns an error after processing an RESP value"),
            }
        }
    }
}

#[derive(Debug, PartialEq)]
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

struct Coordinator {
    replicas: Vec<Participant>,
    rx_resp: mpsc::Receiver<CordMessage>,
    next_sched: usize,
}

impl Coordinator {
    fn new(rx_resp: mpsc::Receiver<CordMessage>) -> Coordinator {
        Coordinator {
            replicas: vec![
                Participant {
                    id: 0,
                    status: Status::Down,
                    address: "127.0.0.1:4444".parse().unwrap(),
                    conn: None,
                },
                Participant {
                    id: 1,
                    status: Status::Down,
                    address: "127.0.0.1:4445".parse().unwrap(),
                    conn: None,
                },
            ],
            rx_resp,
            next_sched: 0,
        }
    }

    async fn run(mut self) -> Result<(), Box<dyn Error>> {
        info!("starting the coordinator");

        self.establish_connection().await;

        info!("established connections to all replicas");

        info!("ready to schedule RESP packets");

        while let Some((resp_value, res_tx)) = self.rx_resp.recv().await {
            // FIXME: schedule_next should return an Option<&Participant>
            // in case where no replica is available.
            let replica = self.schedule_next();
            trace!("scheduling {:?} to replica #{}", resp_value, replica.id);

            let resp_bytes = resp_value.to_bytes();

            // CLEANUP: implement send_frame() method on Participant
            // so that we don't have to make a long chain every time...
            replica
                .conn
                .as_mut()
                .unwrap()
                .send(ProtoValue::Resp(resp_bytes))
                .await
                .unwrap();

            let response = replica
                .conn
                .as_mut()
                .unwrap()
                .next()
                .await
                .unwrap()
                .unwrap();

            let response = match response {
                ProtoValue::Resp(bytes) => RespValue::from_bytes(&bytes),
                _ => panic!("replica replied with non-resp response: {:?}", response),
            };

            res_tx.send(response).unwrap();
        }

        info!("shutting down coordinator");

        Ok(())
    }

    async fn establish_connection(&mut self) {
        for replica in self.replicas.iter_mut() {
            let mut stream;
            loop {
                info!("waiting for participant #{}...", replica.id);
                stream = TcpStream::connect(replica.address).await;
                match stream {
                    Ok(_) => break,
                    Err(_) => tokio::time::sleep(Duration::from_secs(5)).await,
                };
            }

            let codec = ProtoCodec {};
            let conn = codec.framed(stream.unwrap());

            replica.status = Status::Launching;
            replica.conn = Some(conn);
            info!("connected to participant #{}", replica.id);

            /*
            info!("Connected to participant #{}, handshaking...", replica.id);

            let handshake = ProtoValue::Handshake(replica.id);
            replica.send_value(handshake).await;

            let proto_value = replica.receive_value().await;

            match proto_value {
                ProtoValue::Ack(0) => {
                    info!("Participant #{} acked 0, launching", replica.id);
                    replica.status = Status::Launching;
                }
                ProtoValue::Ack(id) => {
                    assert_eq!(replica.id, id);
                    replica.status = Status::Launching;
                }
                _ => {
                    panic!("Invalid response packet from participant #{}", replica.id);
                }
            }
            */
        }
    }

    fn schedule_next(&mut self) -> &mut Participant {
        // FIXME: Don't schedule when there's no running replicas
        let num_replicas = self.replicas.len();
        loop {
            let pa = &self.replicas[self.next_sched as usize];

            // cleanup
            if pa.status == Status::Launching {
                let res = &mut self.replicas[self.next_sched as usize];
                self.next_sched = (self.next_sched + 1) % num_replicas;
                return res;
            } else {
                self.next_sched = (self.next_sched + 1) % num_replicas;
            }
        }
    }
}

impl Participant {
    async fn send_value(&mut self, value: ProtoValue) {
        let conn = self.conn.as_mut().unwrap();
        conn.send(value).await.unwrap(); // FIXME: Handle error
    }

    // FIXME: Return error
    async fn receive_value(&mut self) -> ProtoValue {
        let conn = self.conn.as_mut().unwrap();
        conn.next().await.unwrap().unwrap()
    }
}
