use std::error::Error;

use bytes::BytesMut;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{Decoder, Encoder};
use futures::{stream::StreamExt, SinkExt};

mod kvstore;

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
    let codec = EchoCodec {};
    let mut conn = codec.framed(socket);
    while let Some(message) = conn.next().await {
        if let Ok(message) = message {
            println!("received: {:?}", message);
            conn.send(message).await.unwrap();
        }
    }
}

struct EchoCodec;
type Message = String;

impl Decoder for EchoCodec {
    type Item = Message;

    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        let src = src.split();
        let message = String::from_utf8_lossy(&src).to_string();

        Ok(Some(message))
    }
}

impl Encoder<Message> for EchoCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.extend(item.as_bytes());
        Ok(())
    }
}