use command::Command;
use resp::{RespCodec, RespValue};

use futures::{stream::StreamExt, SinkExt};
use std::error::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Decoder;

mod command;
mod kvstore;
mod resp;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    run_echo_server().await?;
    Ok(())
}

async fn run_echo_server() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:1337").await?;

    loop {
        let (socket, _) = listener.accept().await?;
        process_socket(socket).await;
    }
}

async fn process_socket(socket: TcpStream) {
    let codec = RespCodec {};
    let mut conn = codec.framed(socket);
    while let Some(message) = conn.next().await {
        if let Ok(redis_value) = message {
            println!("received: {:?}", &redis_value);
            let response = if let Ok(command) = Command::from_resp(redis_value) {
                println!("command: {:?}", command);
                RespValue::SimpleString("Ok".into())
            } else {
                RespValue::Error("Invalid Command".into())
            };

            conn.send(response).await.unwrap();
        }
    }
}
