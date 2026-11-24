#![cfg_attr(docsrs, feature(doc_auto_cfg))]

use serde::{Deserialize, Serialize};
use snafu::prelude::*;

pub mod common;
pub mod schema_projection;
pub mod sql;
pub mod util;

pub const DESCRIPTION_METADATA_KEY: &str = "description";

/// Arrow field metadata key holding the column type exactly as the source
/// database reports it (e.g. `numeric(10,2)`, `varchar(50)`, an enum or domain
/// name). The Arrow mapping is lossy, so this preserves the source type for
/// consumers that need it (round-tripping DDL, catalog export, type-aware
/// pushdown).
pub const SOURCE_TYPE_METADATA_KEY: &str = "source_type";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("The database file path is not within the current directory: {path}"))]
    FileNotInDirectory { path: String },
    #[snafu(display("The database file is a symlink: {path}"))]
    FileIsSymlink { path: String },
    #[snafu(display("Error reading file: {source}"))]
    FileReadError { source: std::io::Error },
}

#[derive(PartialEq, Eq, Clone, Copy, Default, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UnsupportedTypeAction {
    #[default]
    Error,
    Warn,
    Ignore,
    String,
}
