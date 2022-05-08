use crate::backend::Backend;
use crate::kvstore::KvStore;
use crate::proto::{ProtoCodec, ProtoValue};
use crate::resp::{RespValue, serialize_redis_value};

use bytes::BytesMut;
use futures::{stream::StreamExt, SinkExt};
use std::{collections::HashMap, error::Error};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Decoder;
use tracing::{info, warn, trace};

pub async fn run() {
    info!("Starting the participant");
    run_participant().await.unwrap();
}

async fn run_participant() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:4444").await?;
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
            trace!("received: {:?}", &proto_value);
            match proto_value {
                ProtoValue::Handshake(id) => {
                    let response = ProtoValue::Ack(backend.id);
                    if backend.id == 0 {
                        info!("Received id: {id}");
                        backend.id = id;
                    }
                    conn.send(response).await.unwrap(); // FIXME: Handle failure
                }
                ProtoValue::Resp(_resp_bytes) => {
                    let resp_value = RespValue::ok("Hey!");
                    let mut bytes = BytesMut::new();
                    serialize_redis_value(&mut bytes, &resp_value);
                    let response = ProtoValue::Resp(bytes.to_vec());
                    conn.send(response).await.unwrap(); // FIXME: Handle failure
                }
                _ => warn!("Unknown proto value: {:?}", proto_value)
            }
        }
    }
}

async fn process_resp<T>(resp_bytes: Vec<u8>, backend: &mut Backend<T>)
where
    T: KvStore,
{
    /*
    let response = if let Ok(command) = Command::from_resp(redis_value) {
        match backend.process_command(command) {
            Ok(v) => v,
            Err(_) => RespValue::err("Backend Error"),
        }
    } else {
        RespValue::Error("Invalid Command".into())
    };
    */
}
