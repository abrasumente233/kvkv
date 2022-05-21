use std::collections::HashMap;

use bytes::{Buf, BufMut, BytesMut};
use serde_derive::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder};

use crate::resp::RespValue;

/// Coordination packet format
#[derive(Serialize, Deserialize, Debug)]
pub enum ProtoValue {
    Handshake(u32),
    Resp(RespValue),
    Replicate(HashMap<String, String>),
}

#[derive(Debug)]
pub struct ProtoCodec;

impl Decoder for ProtoCodec {
    type Item = ProtoValue;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let (proto, bytes_read) = {
            let de = serde_json::Deserializer::from_slice(src);
            let mut value_stream = de.into_iter::<Self::Item>();

            match value_stream.next() {
                Some(Ok(proto_value)) => (proto_value, value_stream.byte_offset()),
                _ => return Ok(None),
            }
        };

        src.advance(bytes_read);
        Ok(Some(proto))
    }
}

impl Encoder<ProtoValue> for ProtoCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: ProtoValue, dst: &mut BytesMut) -> Result<(), Self::Error> {
        serde_json::to_writer(dst.writer(), &item).unwrap();
        Ok(())
    }
}

impl Into<ProtoValue> for RespValue {
    fn into(self) -> ProtoValue {
        ProtoValue::Resp(self)
    }
}