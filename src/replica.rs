use crate::backend::Backend;
use crate::command::Command;
use crate::map::KvStore;
use crate::proto::{ProtoCodec, ProtoValue};
use crate::resp::RespValue;

use futures::{stream::StreamExt, SinkExt};
use std::net::SocketAddrV4;
use std::{collections::HashMap, error::Error};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Decoder;
use tracing::{info, trace, warn};

pub async fn run(port: u16) -> Result<(), Box<dyn Error>> {
    let address = SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, port);
    info!("starting replica on {}", address);
    let listener = TcpListener::bind(address).await?;
    let mut backend = Backend {
        id: 0,
        store: HashMap::new(),
    };

    loop {
        let (socket, _) = listener.accept().await?;
        process_socket(socket, &mut backend).await;
    }
}

async fn process_socket<T>(socket: TcpStream, backend: &mut Backend<T>)
where
    T: KvStore,
{
    let codec = ProtoCodec {};
    let mut conn = codec.framed(socket);
    while let Some(message) = conn.next().await {
        if let Ok(proto_value) = message {
            match proto_value {
                ProtoValue::Handshake(id) => {
                    let response = ProtoValue::Handshake(backend.id);
                    if backend.id == 0 {
                        info!("Received id: {id}");
                        backend.id = id;
                    }
                    conn.send(response).await.unwrap(); // FIXME: Handle failure
                }
                ProtoValue::Resp(resp) => {
                    let response = process_resp(resp, backend).await;
                    conn.send(response).await.unwrap(); // FIXME: Handle failure
                }
                _ => {
                    warn!("Unknown proto value: {:?}", proto_value);
                    let response = RespValue::Error("ERROR".into());
                    conn.send(response.into()).await.unwrap(); // FIXME: Handle failure
                }
            }
        }
    }
}

async fn process_resp<T>(resp_value: RespValue, backend: &mut Backend<T>) -> ProtoValue
where
    T: KvStore,
{
    let command = Command::try_from(resp_value).unwrap();
    trace!("processing command: {:?}", command);

    let response = backend.process_command(command);
    trace!("responding with: {:?}", &response);

    response.into()
}
