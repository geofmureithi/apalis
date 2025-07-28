pub trait MakeShared<Req> {
    type Backend;
    /// The Config for the backend
    type Config;
    /// The error returned if the backend cant be shared
    type MakeError;

    /// Returns the backend to be shared
    fn make_shared(&mut self) -> Result<Self::Backend, Self::MakeError>
    where
        Self::Config: Default,
    {
        self.make_shared_with_config(Default::default())
    }

    /// Returns the backend with config
    fn make_shared_with_config(
        &mut self,
        config: Self::Config,
    ) -> Result<Self::Backend, Self::MakeError>;
}
