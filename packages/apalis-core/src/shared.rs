pub trait MakeShared<Backend> {
    /// The Config for the backend
    type Config;
    /// The error returned if the backend cant be shared
    type MakeError;

    /// Returns the backend to be shared
    fn share(&mut self) -> Result<Backend, Self::MakeError>;

    /// Returns the backend with config
    fn share_with_config(&mut self, config: Self::Config) -> Result<Backend, Self::MakeError>;
}
