use std::vec::IntoIter;

use crate::resp::*;

#[derive(Debug, PartialEq)]
pub enum Command {
    Set(String, String),
    Get(String),
    Del(Vec<String>), // TODO: Try to use SmallVec
}

#[derive(Debug, PartialEq)]
pub enum CommandError {
    InvalidCommand,
}

fn get_command(mut arr: IntoIter<RespValue>) -> Result<Command, CommandError> {
    if let RespValue::BulkString(k) = arr.next().unwrap() {
        return Ok(Command::Get(k));
    }

    Err(CommandError::InvalidCommand)
}

fn set_command(mut arr: IntoIter<RespValue>) -> Result<Command, CommandError> {
    if let RespValue::BulkString(k) = arr.next().unwrap() {
        if let RespValue::BulkString(v) = arr.next().unwrap() {
            return Ok(Command::Set(k, v));
        }
    }

    Err(CommandError::InvalidCommand)
}

fn del_command(arr: IntoIter<RespValue>) -> Result<Command, CommandError> {
    Ok(Command::Del(
        arr.map(|v| match v {
            RespValue::BulkString(k) => k,
            _ => panic!("Oh no"),
        })
        .collect(),
    ))
}

// Cleanup
impl Command {
    pub fn from_resp(value: RespValue) -> Result<Command, CommandError> {
        match value {
            RespValue::Array(arr) => {
                let mut arr = arr.into_iter();
                if let RespValue::BulkString(verb) = arr.next().unwrap() {
                    match verb.as_str() {
                        "GET" => return get_command(arr),
                        "SET" => return set_command(arr),
                        "DEL" => return del_command(arr),
                        _ => (),
                    };
                }
                Err(CommandError::InvalidCommand)
            }
            _ => Err(CommandError::InvalidCommand),
        }
    }
}

// TODO: Add failure tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_get_command() {
        let v = RespValue::from_strs(vec!["GET", "CS"]);
        let cmd = Command::from_resp(v).unwrap();
        assert_eq!(cmd, Command::Get("CS".into()));
    }

    #[test]
    fn parse_set_command() {
        let v = RespValue::from_strs(vec!["SET", "CS", "Cloud Computing"]);
        let cmd = Command::from_resp(v).unwrap();
        assert_eq!(cmd, Command::Set("CS".into(), "Cloud Computing".into()));
    }

    #[test]
    fn parse_del_command() {
        let v = RespValue::from_strs(vec!["DEL", "CS", "Sadness", "Sorrow"]);
        let cmd = Command::from_resp(v).unwrap();
        assert_eq!(
            cmd,
            Command::Del(vec!["CS".into(), "Sadness".into(), "Sorrow".into()])
        );
    }
}
