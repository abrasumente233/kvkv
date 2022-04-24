#![allow(dead_code)]

use std::error::Error;

use bytes::{BytesMut, Buf};
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

fn word(src: &[u8]) -> Option<(&[u8], usize)> {
    let pos = memchr(b'\r', &src)?;

    // FIXME: pos + 2 can overrun the buffer
    Some((&src[..pos], pos + 2))
}

fn int(src: &[u8]) -> Option<(i64, usize)> {
    word(src).and_then(|b| {
        let s = std::str::from_utf8(b.0).unwrap(); // FIXME: Don't unwrap() here.
        Some((s.parse().unwrap(), b.1))
    })
}

// TODO: Eliminate copies!
fn simple_string(src: &[u8]) -> Option<(RedisValue, usize)> {
    word(src).map(|(word, pos)| {
        (
            RedisValue::SimpleString(String::from_utf8_lossy(word).to_string()),
            pos,
        )
    })
}

fn error(src: &[u8]) -> Option<(RedisValue, usize)> {
    word(src).map(|(word, pos)| {
        (
            RedisValue::Error(String::from_utf8_lossy(word).to_string()),
            pos,
        )
    })
}

fn integer(src: &[u8]) -> Option<(RedisValue, usize)> {
    int(src).map(|(i, pos)| (RedisValue::Integer(i), pos))
}

// TODO: More robost error handling
fn bulk_string(src: &[u8]) -> Option<(RedisValue, usize)> {
    // TODO: Use Result to indicate error.
    let (_len, pos_len) = int(src)?; // @TODO: Check length
    let (data, pos_data) = word(&src[pos_len..])?;
    
    let s = String::from_utf8_lossy(&data).to_string(); // TODO: Eliminate copy

    Some((RedisValue::BulkString(s), pos_len + pos_data))
}

impl Decoder for RedisCodec {
    type Item = RedisValue;

    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        let first_byte = src[0];
        let remain = &src[1..];

        let result = match first_byte {
            b'+' => simple_string(remain),
            b'-' => error(remain),
            b':' => integer(remain),
            b'$' => bulk_string(remain),
            _ => panic!("Unknown first byte"), // TODO: Be a nice Rust citizen, don't panic
        };

        match result {
            Some((value, pos)) => {
                src.advance(pos + 1); // plus the first byte
                Ok(Some(value))
            },
            None =>
                Ok(None)
            ,
        }
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
