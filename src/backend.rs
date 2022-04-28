use crate::{command::Command, kvstore::KvStore, resp::RespValue};

pub(crate) struct Backend<T>
where
    T: KvStore,
{
    pub id: u32,
    pub store: T,
}

#[derive(Debug)]
pub(crate) enum KvError {
    InvalidKey,
}

impl<T> Backend<T>
where
    T: KvStore,
{
    pub fn process_command(&mut self, cmd: Command) -> Result<RespValue, KvError> {
        use Command::*;
        match cmd {
            Get(k) => self.process_get(k),
            Set(k, v) => self.process_set(k, v),
            Del(keys) => self.process_del(keys),
        }
    }

    fn process_get(&mut self, k: String) -> Result<RespValue, KvError> {
        match self.store.kv_get(k.as_str()) {
            Some(v) => Ok(RespValue::from_strs(vec!["GET", v])),
            None => Err(KvError::InvalidKey),
        }
    }

    fn process_set(&mut self, k: String, v: String) -> Result<RespValue, KvError> {
        self.store.kv_put(k.as_str(), v.as_str());
        Ok(RespValue::ok("OK"))
    }

    fn process_del(&mut self, keys: Vec<String>) -> Result<RespValue, KvError> {
        // FIXME: Report how many keys are succussfully deleted
        for key in keys {
            self.store.kv_del(key.as_str());
        }

        Ok(RespValue::ok("OK"))
    }
}
