use crate::{command::Command, map::KvStore, resp::RespValue};

pub(crate) struct Backend<T>
where
    T: KvStore,
{
    pub id: u32,
    pub store: T,
}

impl<T> Backend<T>
where
    T: KvStore,
{
    pub fn process_command(&mut self, cmd: Command) -> RespValue {
        use Command::*;
        match cmd {
            Get(k) => self.process_get(k),
            Set(k, v) => self.process_set(k, v),
            Del(keys) => self.process_del(keys),
        }
    }

    fn process_get(&mut self, k: String) -> RespValue {
        match self.store.kv_get(k.as_str()) {
            Some(v) => RespValue::array(&[v]),
            None => RespValue::array(&["nil"]),
        }
    }

    fn process_set(&mut self, k: String, v: String) -> RespValue {
        self.store.kv_put(k.as_str(), v.as_str());
        RespValue::SimpleString("OK".into())
    }

    fn process_del(&mut self, keys: Vec<String>) -> RespValue {
        // FIXME: Report how many keys are succussfully deleted
        let mut num_deleted = 0;
        for key in keys {
            num_deleted += self.store.kv_del(key.as_str()) as i64;
        }

        RespValue::Integer(num_deleted)
    }
}
