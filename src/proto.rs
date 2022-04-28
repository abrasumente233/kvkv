use std::collections::HashMap;

use bytes::{Buf, BufMut, BytesMut};
use serde_derive::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder};

#[derive(Serialize, Deserialize, Debug)]
/// Coordination packet format
pub enum ProtoValue {
    Handshake(u32),

    // Replica responds `Ack` when getting `Handshake` or `Launch`,
    // `Ack(0)` means the replica just started and have no data,
    // `Ack(other)` means the replica is running, and `other` is
    // the replica ID.
    Ack(u32),

    Resp(Vec<u8>),
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
