use bytes::{Buf, BytesMut};
use memchr::memchr;
use tokio_util::codec::{Decoder, Encoder};

pub struct RespCodec;

#[derive(Debug)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(String),
    Array(Vec<RespValue>),
}

impl RespValue {
    pub(crate) fn ok(msg: &str) -> RespValue {
        RespValue::SimpleString(msg.into())
    }

    pub(crate) fn err(msg: &str) -> RespValue {
        RespValue::Error(msg.into())
    }

    pub(crate) fn from_strs(command: Vec<&str>) -> RespValue {
        RespValue::Array(
            command
                .into_iter()
                .map(|s| RespValue::BulkString(s.to_string()))
                .collect(),
        )
    }

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
fn simple_string(src: &[u8]) -> Option<(RespValue, usize)> {
    word(src).map(|(word, pos)| {
        (
            RespValue::SimpleString(String::from_utf8_lossy(word).to_string()),
            pos,
        )
    })
}

fn error(src: &[u8]) -> Option<(RespValue, usize)> {
    word(src).map(|(word, pos)| {
        (
            RespValue::Error(String::from_utf8_lossy(word).to_string()),
            pos,
        )
    })
}

fn integer(src: &[u8]) -> Option<(RespValue, usize)> {
    int(src).map(|(i, pos)| (RespValue::Integer(i), pos))
}

// TODO: More robost error handling
fn bulk_string(src: &[u8]) -> Option<(RespValue, usize)> {
    // TODO: Use Result to indicate error.
    let (_len, pos_len) = int(src)?; // @TODO: Check length
    let (data, pos_data) = word(&src[pos_len..])?;

    let s = String::from_utf8_lossy(&data).to_string(); // TODO: Eliminate copy

    Some((RespValue::BulkString(s), pos_len + pos_data))
}

fn array(src: &[u8]) -> Option<(RespValue, usize)> {
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

    Some((RespValue::Array(array), total_pos))
}

fn parse(src: &[u8]) -> Option<(RespValue, usize)> {
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

fn serialize_redis_value(dst: &mut BytesMut, value: &RespValue) {
    match value {
        RespValue::SimpleString(s) => {
            dst.extend_from_slice(b"+");
            dst.extend_from_slice(s.as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        RespValue::Error(s) => {
            dst.extend_from_slice(b"-");
            dst.extend_from_slice(s.as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        RespValue::Integer(i) => {
            dst.extend_from_slice(b":");
            dst.extend_from_slice(format!("{}", i).as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        RespValue::BulkString(s) => {
            dst.extend_from_slice(b"$");
            dst.extend_from_slice(format!("{}", s.len()).as_bytes());
            dst.extend_from_slice(b"\r\n");
            dst.extend_from_slice(s.as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        RespValue::Array(arr) => {
            dst.extend_from_slice(b"*");
            dst.extend_from_slice(format!("{}", arr.len()).as_bytes());
            dst.extend_from_slice(b"\r\n");
            for it in arr.iter() {
                serialize_redis_value(dst, it);
            }
        }
    }
}

impl Decoder for RespCodec {
    type Item = RespValue;

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

impl Encoder<RespValue> for RespCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: RespValue, dst: &mut BytesMut) -> Result<(), Self::Error> {
        serialize_redis_value(dst, &item);
        Ok(())
    }
}
