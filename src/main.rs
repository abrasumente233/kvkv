use std::error::Error;
use futures::{stream::StreamExt, SinkExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Decoder;

mod kvstore;
mod resp;
mod command;

use crate::resp::RespCodec;

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
            println!("received: {:?}", redis_value);
            conn.send(redis_value).await.unwrap();
        }
    }
}