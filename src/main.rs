#![allow(dead_code)]

use std::error::Error;

use bytes::BytesMut;
use futures::stream::StreamExt;
use memchr::memchr;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Decoder;

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
    let codec = RedisCodec {};
    let mut conn = codec.framed(socket);
    while let Some(message) = conn.next().await {
        if let Ok(redis_value) = message {
            println!("received: {:?}", redis_value);
        }
    }
}

struct RedisCodec;

#[derive(Debug)]
enum RedisValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(String),
    Array(Vec<RedisValue>),
}

fn word(src: &mut BytesMut) -> Option<BytesMut> {
    let pos = memchr(b'\r', &src)?;

    // FIXME: pos + 2 can overrun the buffer
    let mut word = src.split_to(pos + 2);
    let word = word.split_to(pos);

    Some(word)
}

fn int(src: &mut BytesMut) -> Option<i64> {
    word(src).and_then(|b| {
        let s = std::str::from_utf8(&b).unwrap(); // FIXME: Don't unwrap() here.
        Some(s.parse().unwrap())
    })
}

// TODO: Eliminate copies!
fn simple_string(src: &mut BytesMut) -> Option<RedisValue> {
    Some(RedisValue::SimpleString(String::from_utf8_lossy(&word(src)?).to_string()))
}

fn error(src: &mut BytesMut) -> Option<RedisValue> {
    Some(RedisValue::Error(String::from_utf8_lossy(&word(src)?).to_string()))
}

fn integer(src: &mut BytesMut) -> Option<RedisValue> {
    Some(RedisValue::Integer(int(src)?))
}

// TODO: More robost error handling
fn bulk_string(src: &mut BytesMut) -> Option<RedisValue> {
    // TODO: Use Result to indicate error.
    let _len = int(src)?; // @TODO: Check length
    let data = word(src)?;
    let s = String::from_utf8_lossy(&data).to_string(); // TODO: Eliminate copy
    
    Some(RedisValue::BulkString(s))
}

impl Decoder for RedisCodec {
    type Item = RedisValue;

    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        let first = src.split_to(1);

        Ok(match first[0] {
            b'+' => simple_string(src),
            b'-' => error(src),
            b':' => integer(src),
            b'$' => bulk_string(src),
            _ => panic!("Unknown first byte"), // TODO: Be a nice Rust citizen, don't panic
        })
    }
}

/*
impl Encoder<RedisValue> for RedisCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: RedisValue, dst: &mut BytesMut) -> Result<(), Self::Error> {
        Ok(())
    }
}
*/