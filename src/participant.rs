use crate::backend::Backend;
use crate::command::Command;
use crate::kvstore::KvStore;
use crate::resp::{RespCodec, RespValue};

use futures::{stream::StreamExt, SinkExt};
use std::{collections::HashMap, error::Error};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Decoder;
use tracing::info;

pub async fn run() {
    info!("Starting the participant");
    run_participant().await.unwrap();
}

async fn run_participant() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:1337").await?;
    let mut backend = Backend {
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
    let codec = RespCodec {};
    let mut conn = codec.framed(socket);
    while let Some(message) = conn.next().await {
        if let Ok(redis_value) = message {
            println!("received: {:?}", &redis_value);
            let response = if let Ok(command) = Command::from_resp(redis_value) {
                match backend.process_command(command) {
                    Ok(v) => v,
                    Err(_) => RespValue::err("Backend Error"),
                }
            } else {
                RespValue::Error("Invalid Command".into())
            };

            conn.send(response).await.unwrap();
        }
    }
}
