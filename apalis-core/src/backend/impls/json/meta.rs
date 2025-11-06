use serde::{Serialize, de::DeserializeOwned};

pub(super) type JsonMapMetadata = serde_json::Map<String, serde_json::Value>;

impl<T> crate::task::metadata::MetadataExt<T> for JsonMapMetadata
where
    T: Serialize + DeserializeOwned,
{
    type Error = serde_json::Error;

    fn extract(&self) -> Result<T, serde_json::Error> {
        use serde::de::Error as _;
        let key = std::any::type_name::<T>();
        match self.get(key) {
            Some(value) => T::deserialize(value),
            None => Err(serde_json::Error::custom(format!(
                "No entry for type `{key}` in metadata"
            ))),
        }
    }

    fn inject(&mut self, value: T) -> Result<(), serde_json::Error> {
        let key = std::any::type_name::<T>();
        let json_value = serde_json::to_value(value)?;
        self.insert(key.to_string(), json_value);
        Ok(())
    }
}
