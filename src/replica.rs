use crate::backend::Backend;
use crate::command::Command;
use crate::map::KvStore;
use crate::proto::{read_frame, write_frame, ProtoCodec, ProtoValue};
use crate::resp::RespValue;

use futures::{stream::StreamExt, SinkExt};
use std::net::SocketAddrV4;
use std::{collections::HashMap, error::Error};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Decoder;
use tracing::{info, trace, warn, instrument};

pub async fn run(port: u16) -> Result<(), Box<dyn Error>> {
    let address = SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, port);
    info!("starting replica on {}", address);
    let listener = TcpListener::bind(address).await?;
    let mut backend = Backend {
        id: u32::MAX,
        store: HashMap::new(),
    };

    loop {
        let (socket, _) = listener.accept().await?;
        handle_socket(socket, &mut backend).await;
    }
}

#[instrument(skip(socket, backend))]
async fn handle_socket<T>(socket: TcpStream, backend: &mut Backend<T>)
where
    T: KvStore,
{
    let codec = ProtoCodec {};
    let mut conn = codec.framed(socket);
    loop {
        let proto_value = read_frame(&mut conn).await.unwrap();
        match proto_value {
            ProtoValue::Handshake(id) => {
                trace!("Handshake({id})");
                let response = ProtoValue::Handshake(backend.id);
                backend.id = id;
                write_frame(&mut conn, response).await.unwrap();
            }
            ProtoValue::Resp(resp) => {
                if resp.is_write() {
                    handle_write(&mut conn, backend, resp).await.unwrap();
                } else {
                    let response = process_resp(resp, backend);
                    write_frame(&mut conn, response.into()).await.unwrap();
                }
            }
            _ => {
                warn!("Unknown proto value: {:?}", proto_value);
                let response = RespValue::Error("ERROR".into());
                conn.send(response.into()).await.unwrap(); // FIXME: Handle failure
            }
        }
    }
}

// cleanup
#[instrument(skip(conn, backend))]
async fn handle_write<T, F, E>(
    conn: &mut F,
    backend: &mut Backend<T>,
    resp: RespValue,
) -> Result<(), Box<dyn Error>>
where
    T: KvStore,
    F: StreamExt<Item = Result<ProtoValue, E>>
        + SinkExt<ProtoValue>
        + Unpin
        + futures::Sink<ProtoValue, Error = E>,
    E: Error,
{
    // Vote yes
    let vote = ProtoValue::Vote(true);
    write_frame(conn, vote).await.unwrap();
    trace!("voted yes");

    // Wait for final decision
    let decision = read_frame(conn).await.unwrap();

    // Execute the decision and send final response
    let response = match decision {
        ProtoValue::Decision(true) => {
            trace!("master says commit");
            process_resp(resp, backend)
        }
        ProtoValue::Decision(false) => {
            trace!("master says abort");
            ProtoValue::Decision(false) // echo as ACK
        }
        _ => unreachable!(),
    };
    write_frame(conn, response).await.unwrap();

    Ok(())
}

fn process_resp<T>(resp_value: RespValue, backend: &mut Backend<T>) -> ProtoValue
where
    T: KvStore,
{
    let response = match Command::try_from(resp_value) {
        Ok(cmd) => {
            trace!("command: {:?}", &cmd);
            backend.process_command(cmd)
        }
        Err(_) => RespValue::Error("ERROR".into()),
    }
    .into();

    trace!("respond: {:?}", &response);

    response
}
