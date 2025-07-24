

#[derive(PartialEq, Eq, Clone, Copy, Default, Debug)]
pub enum UnsupportedTypeAction {
    /// Refuse to create the table if any unsupported types are found
    #[default]
    Error,
    /// Log a warning for any unsupported types
    Warn,
    /// Ignore any unsupported types (i.e. skip them)
    Ignore,
    /// Attempt to convert any unsupported types to a string
    String,
}
