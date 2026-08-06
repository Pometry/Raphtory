use super::GqlGraphError;
use crate::data::Data;
use dynamic_graphql::{Context, OneOfInput, Result, Upload};
use std::{fs, io, path::PathBuf};
use tempfile::TempDir;

/// Where a loader reads its parquet from: a directory already on the server, or
/// data supplied with the request.
#[derive(OneOfInput)]
#[graphql(name = "DataSource")]
pub enum GqlDataSource {
    /// Path to a parquet directory on the server. Subject to the parquet allowlist.
    Path(String),
    /// Multipart upload of a single parquet file.
    Upload(Upload),
}

/// A parquet directory plus, for uploads, the temp dir that owns it. Dropping the
/// guard removes the spooled file, so callers must hold it until the load returns.
pub struct ParquetInput {
    pub path: PathBuf,
    _guard: Option<TempDir>,
}

impl ParquetInput {
    /// A client-supplied path is only usable if the allowlist permits it, so the
    /// check belongs here rather than at the call site: no caller can hold an
    /// unvalidated path input. An upload's path is server-chosen and so exempt.
    pub(crate) async fn from_path(data: &Data, path: String) -> Result<Self> {
        let path = PathBuf::from(path);
        data.is_parquet_path_allowed(&path)
            .await
            .map_err(|e| GqlGraphError::LoadError(e.to_string()))?;
        Ok(Self { path, _guard: None })
    }

    /// The loaders take a directory, so the uploaded file is placed inside one.
    pub(crate) fn from_upload(ctx: &Context<'_>, upload: Upload) -> Result<Self> {
        let mut content = upload.value(ctx)?.content;
        let dir = TempDir::new()?;
        let path = dir.path().join("part.parquet");
        let mut out = fs::File::create(&path)?;
        io::copy(&mut content, &mut out)?;
        out.sync_all()?;
        Ok(Self {
            path: dir.path().to_path_buf(),
            _guard: Some(dir),
        })
    }
}
