#![allow(dead_code)]

use std::{error::Error, vec::IntoIter};

use bytes::{Buf, BytesMut};
use futures::{stream::StreamExt, SinkExt};
use memchr::memchr;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{Decoder, Encoder};

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
            conn.send(redis_value).await.unwrap();
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

fn array(src: &[u8]) -> Option<(RedisValue, usize)> {
    let mut total_pos = 0;
    let (len, pos_len) = int(src)?;

    // TODO: It's becoming tedious to manually update total pos,
    // and advance buffer cursor.
    // It would be great if we have one type that does it for us.
    total_pos += pos_len;
    let mut src = &src[pos_len..];

    let mut array = Vec::new();
    array.reserve(len as usize);

    for _ in 0..len {
        let (value, data_len) = parse(src)?;
        array.push(value);
        src = &src[data_len..]; // FIXME: Cleanup
        total_pos += data_len;
    }

    Some((RedisValue::Array(array), total_pos))
}

fn parse(src: &[u8]) -> Option<(RedisValue, usize)> {
    if src.len() == 0 {
        return None;
    }

    let first_byte = src[0];
    let remain = &src[1..];

    match first_byte {
        b'+' => simple_string(remain),
        b'-' => error(remain),
        b':' => integer(remain),
        b'$' => bulk_string(remain),
        b'*' => array(remain),
        _ => None, // TODO: Report more concrete error.
    }
    .map(|(v, p)| (v, p + 1)) // Plus the first byte
}

fn serialize_redis_value(dst: &mut BytesMut, value: &RedisValue) {
    match value {
        RedisValue::SimpleString(s) => {
            dst.extend_from_slice(b"+");
            dst.extend_from_slice(s.as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        RedisValue::Error(s) => {
            dst.extend_from_slice(b"-");
            dst.extend_from_slice(s.as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        RedisValue::Integer(i) => {
            dst.extend_from_slice(b":");
            dst.extend_from_slice(format!("{}", i).as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        RedisValue::BulkString(s) => {
            dst.extend_from_slice(b"$");
            dst.extend_from_slice(format!("{}", s.len()).as_bytes());
            dst.extend_from_slice(b"\r\n");
            dst.extend_from_slice(s.as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        RedisValue::Array(arr) => {
            dst.extend_from_slice(b"*");
            dst.extend_from_slice(format!("{}", arr.len()).as_bytes());
            dst.extend_from_slice(b"\r\n");
            for it in arr.iter() {
                serialize_redis_value(dst, it);
            }
        }
    }
}

impl Decoder for RedisCodec {
    type Item = RedisValue;

    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match parse(src) {
            Some((value, pos)) => {
                src.advance(pos);
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }
}

impl Encoder<RedisValue> for RedisCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: RedisValue, dst: &mut BytesMut) -> Result<(), Self::Error> {
        serialize_redis_value(dst, &item);
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
enum Command {
    Set(String, String),
    Get(String),
    Del(Vec<String>), // TODO: Try to use SmallVec
}

#[derive(Debug, PartialEq)]
enum RedisError {
    InvalidCommand,
}

fn get_command(mut arr: IntoIter<RedisValue>) -> Result<Command, RedisError> {
    if let RedisValue::BulkString(k) = arr.next().unwrap() {
        return Ok(Command::Get(k));
    }

    Err(RedisError::InvalidCommand)
}

fn set_command(mut arr: IntoIter<RedisValue>) -> Result<Command, RedisError> {
    if let RedisValue::BulkString(k) = arr.next().unwrap() {
        if let RedisValue::BulkString(v) = arr.next().unwrap() {
            return Ok(Command::Set(k, v));
        }
    }

    Err(RedisError::InvalidCommand)
}

fn del_command(arr: IntoIter<RedisValue>) -> Result<Command, RedisError> {
    Ok(Command::Del(
        arr.map(|v| match v {
            RedisValue::BulkString(k) => k,
            _ => panic!("Oh no"),
        })
        .collect(),
    ))
}

// Cleanup
impl Command {
    fn from_resp(value: RedisValue) -> Result<Command, RedisError> {
        match value {
            RedisValue::Array(arr) => {
                let mut arr = arr.into_iter();
                if let RedisValue::BulkString(verb) = arr.next().unwrap() {
                    if verb == "GET" {
                        return get_command(arr);
                    } else if verb == "SET" {
                        return set_command(arr);
                    } else if verb == "DEL" {
                        return del_command(arr);
                    }
                }
                Err(RedisError::InvalidCommand)
            }
            _ => Err(RedisError::InvalidCommand),
        }
    }
}

// TODO: Add failure tests
#[cfg(test)]
mod tests {
    use super::*;

    fn command_resp(command: Vec<&str>) -> RedisValue {
        RedisValue::Array(
            command
                .into_iter()
                .map(|s| RedisValue::BulkString(s.to_string()))
                .collect(),
        )
    }

    #[test]
    fn parse_get_command() {
        let v = command_resp(vec!["GET", "CS"]);
        let cmd = Command::from_resp(v).unwrap();
        assert_eq!(cmd, Command::Get("CS".into()));
    }

    #[test]
    fn parse_set_command() {
        let v = command_resp(vec!["SET", "CS", "Cloud Computing"]);
        let cmd = Command::from_resp(v).unwrap();
        assert_eq!(cmd, Command::Set("CS".into(), "Cloud Computing".into()));
    }

    #[test]
    fn parse_del_command() {
        let v = command_resp(vec!["DEL", "CS", "Sadness", "Sorrow"]);
        let cmd = Command::from_resp(v).unwrap();
        assert_eq!(cmd, Command::Del(vec!["CS".into(), "Sadness".into(), "Sorrow".into()]));
    }
}
