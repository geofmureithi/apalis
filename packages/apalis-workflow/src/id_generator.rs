use apalis_core::task::task_id::RandomId;

pub trait GenerateId {
    fn generate() -> Self;
}

#[cfg(feature = "uuid")]
impl GenerateId for uuid::Uuid {
    fn generate() -> Self {
        uuid::Uuid::new_v4()
    }
}

#[cfg(feature = "ulid")]
impl GenerateId for ulid::Ulid {
    fn generate() -> Self {
        ulid::Ulid::new()
    }
}

impl GenerateId for RandomId {
    fn generate() -> Self {
        RandomId::default()
    }
}
