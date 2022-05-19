use crate::backend::Backend;
use crate::command::Command;
use crate::kvstore::KvStore;
use crate::proto::{ProtoCodec, ProtoValue};
use crate::resp::{serialize_redis_value, RespValue};

use bytes::BytesMut;
use futures::{stream::StreamExt, SinkExt};
use std::{collections::HashMap, error::Error};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Decoder;
use tracing::{info, trace, warn};

pub async fn run(port: u16) -> Result<(), Box<dyn Error>> {
    let address = format!("127.0.0.1:{}", port);
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
                    let response = ProtoValue::Ack(backend.id);
                    if backend.id == 0 {
                        info!("Received id: {id}");
                        backend.id = id;
                    }
                    conn.send(response).await.unwrap(); // FIXME: Handle failure
                }
                ProtoValue::Resp(resp_bytes) => {
                    let response = process_resp(resp_bytes, backend).await;
                    conn.send(response).await.unwrap(); // FIXME: Handle failure
                }
                _ => warn!("Unknown proto value: {:?}", proto_value),
            }
        }
    }
}

async fn process_resp<T>(resp_bytes: Vec<u8>, backend: &mut Backend<T>) -> ProtoValue
where
    T: KvStore,
{
    let resp_value = RespValue::from_bytes(&resp_bytes);
    let command = Command::from_resp(resp_value).unwrap();
    trace!("processing command: {:?}", command);
    let result = backend.process_command(command);
    trace!("responding with: {:?}", &result);
    let mut bytes = BytesMut::new();
    serialize_redis_value(&mut bytes, &result);
    ProtoValue::Resp(bytes.to_vec())
}
