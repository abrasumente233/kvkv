pub trait KvStore {
    fn kv_get(&self, key: &str) -> Option<&str>;
    fn kv_put(&mut self, key: &str, value: &str);
    /// Returns true if the key was in the map.
    fn kv_del(&mut self, key: &str) -> bool;
}

impl KvStore for std::collections::HashMap<String, String> {
    #[inline]
    fn kv_get(&self, key: &str) -> Option<&str> {
        self.get(key).and_then(|s| Some(s.as_str()))
    }

    #[inline]
    fn kv_put(&mut self, key: &str, value: &str) {
        self.insert(key.into(), value.into());
    }

    #[inline]
    fn kv_del(&mut self, key: &str) -> bool {
        self.remove(key).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_kv_trait() {
        let mut hm: HashMap<String, String> = HashMap::new();
        assert_eq!(hm.kv_get("Changsha"), None);
        hm.kv_put("Changsha", "Rainy");
        assert_eq!(hm.kv_get("Changsha"), Some("Rainy"));
        hm.kv_put("Changsha", "Sunny");
        assert_eq!(hm.kv_get("Changsha"), Some("Sunny"));
        hm.kv_del("Changsha");
        assert_eq!(hm.kv_get("Changsha"), None);
    }
}
