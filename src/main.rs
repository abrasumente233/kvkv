use std::error::Error;

use tokio::net::{TcpListener, TcpStream};

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

async fn process_socket(mut socket: TcpStream) {
    let (mut rx, mut tx) = socket.split();
    tokio::io::copy(&mut rx, &mut tx).await.unwrap();
}