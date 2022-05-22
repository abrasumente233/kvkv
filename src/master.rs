#![allow(dead_code)]

use std::{
    error::Error,
    net::{SocketAddr, SocketAddrV4},
    time::Duration,
};

use futures::{SinkExt, StreamExt};
use tokio::{
    join,
    net::{TcpListener, TcpStream},
    spawn,
    sync::{
        mpsc,
        oneshot::{self, error::RecvError},
    },
};
use tokio_util::codec::{Decoder, Framed};
use tracing::{error, info, trace, warn};

use crate::{
    proto::{read_frame, write_frame, ProtoCodec, ProtoValue},
    resp::{RespCodec, RespValue},
};

/// A [ProtoValue] message that can be sent to a [Master],
/// who manages all connections to the [Replica]s.
///
/// After sending the RESP message, the [Master] will
/// wait for a response from the [Replica], and use the
/// provided `oneshot` to send the response back to the message sender.
///
/// [Master]: ../master/struct.Master.html
/// [Replica]: ../master/struct.Replica.html
/// [ProtoValue]: ../proto/enum.ProtoValue.html
type MasterMessage = (ProtoValue, oneshot::Sender<ProtoValue>);

pub async fn run(port: u16, replica_addrs: Vec<String>) -> Result<(), Box<dyn Error>> {
    let (tx_resp, master_chan) = mpsc::channel::<MasterMessage>(16);

    let values = join!(
        spawn(async move { Master::new(master_chan, replica_addrs).run().await.unwrap() }),
        spawn(async move { listen_for_clients(tx_resp, port).await.unwrap() })
    );

    (values.0?, values.1?);
    Ok(())
}

async fn listen_for_clients(
    tx_resp: mpsc::Sender<MasterMessage>,
    port: u16,
) -> Result<(), Box<dyn Error>> {
    let address = SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, port);
    info!("starting master on {}", address);
    let listener = TcpListener::bind(address).await?;

    loop {
        let tx_resp = tx_resp.clone();
        let (socket, _) = listener.accept().await?;
        handle_client_socket(socket, tx_resp).await;
    }
}

async fn handle_client_socket(socket: TcpStream, master: mpsc::Sender<MasterMessage>) {
    let codec = RespCodec {};
    let mut conn = codec.framed(socket);
    loop {
        let resp = read_frame(&mut conn).await.unwrap();
        match talk_to_master(&master, resp.into()).await {
            Ok(ProtoValue::Resp(resp)) => write_frame(&mut conn, resp).await.unwrap(), // FIXME: handle connection error
            Ok(_) => panic!("replica returns non-resp value"),
            Err(_) => panic!("sender dropped"),
        }
    }
}

// I should really come up with a better name for this :)
async fn talk_to_master(
    master: &mpsc::Sender<MasterMessage>,
    proto_value: ProtoValue,
) -> Result<ProtoValue, RecvError> {
    let (tx, res) = oneshot::channel();

    // We just unwrap because we don't have much to do
    // if inter task communication failed...
    master.send((proto_value, tx)).await.unwrap();

    res.await
}

#[derive(Debug, PartialEq)]
enum Status {
    Offline,
    Online,
    Recover,
}

#[derive(Debug)]
struct Replica {
    id: u32,
    status: Status,
    addr: SocketAddr,
    conn: Option<Framed<TcpStream, ProtoCodec>>,
}

struct Master {
    replicas: Vec<Replica>,
    master_chan: mpsc::Receiver<MasterMessage>,
    next_sched: u32,
    written: bool,
}

impl Master {
    fn new(master_chan: mpsc::Receiver<MasterMessage>, replica_addrs: Vec<String>) -> Master {
        let replicas = replica_addrs
            .into_iter()
            .enumerate()
            .map(|(id, addr)| Replica {
                id: id as u32,
                status: Status::Offline,
                addr: addr.parse().unwrap(),
                conn: None,
            })
            .collect();

        Master {
            replicas,
            master_chan,
            next_sched: 0,
            written: false,
        }
    }

    async fn run(mut self) -> Result<(), Box<dyn Error>> {
        info!("starting the master");

        self.connect_all().await;

        info!("accepting RESP messages");

        while let Some((proto_value, res_chan)) = self.master_chan.recv().await {
            // FIXME: shouldn't pick a replica if it's write operation
            let replica = self.schedule_next();

            match replica {
                Some(replica) => {
                    trace!("scheduling {:?} to replica #{}", proto_value, replica.id);

                    let response: ProtoValue = match proto_value {
                        ProtoValue::Resp(resp) => {
                            if resp.is_write() {
                                // FIXME: handle connection error
                                self.do_write(resp.into()).await.unwrap()
                            } else {
                                replica.talk(resp.into()).await.unwrap()
                            }
                        }
                        _ => unreachable!(),
                    };

                    match response {
                        ProtoValue::Resp(_) => (),
                        _ => panic!("replica replied with non-resp response: {:?}", response),
                    }

                    // FIXME: Sometimes the message sender doesn't
                    // want the response, and drops the res_rx, in
                    // which case we shouldn't unwrap() directly?
                    res_chan.send(response).unwrap();
                }
                None => {
                    warn!("no replica available");
                    res_chan
                        .send(RespValue::Error("ERROR".into()).into())
                        .unwrap();
                }
            }
        }

        info!("shutting down master");

        Ok(())
    }

    // Implements two-phase commit
    // FIXME: Only send messages to replicas that are ONLINE
    async fn do_write(&mut self, value: ProtoValue) -> Result<ProtoValue, std::io::Error> {
        // Step 1: send write to all replicas
        let mut all_yes = true;
        for r in self.replicas.iter_mut() {
            // Step 2: wait for all replicas to reply
            // Perf: don't clone
            let response = r.talk(value.clone()).await.unwrap();
            match response {
                ProtoValue::Vote(vote) => all_yes &= vote,
                _ => unreachable!(),
            }
        }

        // Step 3: if all replicas agree, write to all replicas
        let decision = ProtoValue::Decision(all_yes);
        let mut res = ProtoValue::Handshake(444);
        for r in self.replicas.iter_mut() {
            let response = r.talk(decision.clone()).await.unwrap();
            match response {
                ProtoValue::Resp(resp) => res = resp.into(),
                _ => unreachable!(),
            }
        }

        Ok(res)
    }

    async fn connect_all(&mut self) {
        for r in self.replicas.iter_mut() {
            // FIXME: handle connection error
            r.try_connect(self.written).await.unwrap();
        }

        info!("established connections to all replicas");
    }

    fn schedule_next(&mut self) -> Option<&mut Replica> {
        let n = self.replicas.len();
        let r = self
            .replicas
            .iter_mut()
            .filter(|r| r.id >= self.next_sched && r.status == Status::Online)
            .next();

        // Oh no
        if let Some(r) = r {
            self.next_sched = (r.id + 1) % n as u32;
            Some(r)
        } else {
            self.next_sched = 0;
            None
        }
    }
}

impl Replica {
    async fn write_frame(&mut self, value: ProtoValue) -> Result<(), std::io::Error> {
        let conn = self.conn.as_mut().unwrap();
        conn.send(value).await
    }

    async fn read_frame(&mut self) -> Option<Result<ProtoValue, std::io::Error>> {
        let conn = self.conn.as_mut().unwrap();
        conn.next().await
    }

    async fn try_connect(&mut self, written: bool) -> Result<(), std::io::Error> {
        let mut stream;
        loop {
            info!("waiting for R{} on {}", self.id, self.addr);
            stream = TcpStream::connect(self.addr).await;
            match stream {
                Ok(_) => break,
                Err(_) => tokio::time::sleep(Duration::from_secs(5)).await,
            };
        }

        let codec = ProtoCodec {};
        let conn = codec.framed(stream.unwrap());
        self.conn = Some(conn);
        self.status = Status::Online;

        let response = self.talk(ProtoValue::Handshake(self.id)).await?;

        match response {
            ProtoValue::Handshake(u32::MAX) => {
                info!("R{} ack u32::MAX, fresh starting", self.id);

                // Depend on whether any write operations happened,
                // we need to restore the data to the replica.
                if written {
                    self.status = Status::Recover;
                    unimplemented!();
                } else {
                    self.status = Status::Online;
                }
            }
            ProtoValue::Handshake(id) => {
                info!("reconnected to R{}", id);
                assert_eq!(self.id, id);
                self.status = Status::Online;
            }
            _ => {
                error!(
                    "R{} should have replied with Handshake, but replied with {:?}",
                    self.id, response
                );
                panic!("Invalid response packet from R{}", self.id);
            }
        }

        Ok(())
    }

    async fn talk(&mut self, value: ProtoValue) -> Result<ProtoValue, std::io::Error> {
        self.write_frame(value).await?;
        // FIXME: return connection error
        Ok(self.read_frame().await.unwrap()?)
    }
}
