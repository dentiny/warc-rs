//! Streaming storage inputs for WARC readers. Inputs are reopened on demand.

use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub use opendal;
use opendal::layers::{RetryLayer, TimeoutLayer};
use opendal::{FuturesAsyncReader, Operator};
use url::Url;

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

/// A reopenable storage object.
#[derive(Clone, Debug)]
pub struct WarcFile {
    storage: Storage,
    path: String,
    source_uri: String,
}

#[derive(Clone, Debug)]
enum Storage {
    Operator(Operator),
    Local(PathBuf),
}

impl WarcFile {
    /// Use an existing operator, including caller-configured S3 credentials and transport.
    /// Default timeout and jittered retry layers are added to this operator clone.
    pub fn new(operator: Operator, path: impl Into<String>) -> io::Result<Self> {
        let path = path.into();
        if path.is_empty() || path.ends_with('/') {
            return Err(invalid("WARC input must name an object"));
        }
        opendal::install_default();
        let operator = operator.layer(TimeoutLayer::default()).layer(
            RetryLayer::new()
                .with_jitter()
                .with_max_times(10)
                .with_max_delay(Duration::from_secs(30)),
        );
        Ok(Self {
            storage: Storage::Operator(operator),
            source_uri: path.clone(),
            path,
        })
    }

    /// Describe a local path or file URL. No I/O happens here.
    pub fn from_uri(uri: &str) -> io::Result<Self> {
        if uri.is_empty() {
            return Err(invalid("WARC URI must not be empty"));
        }
        let path = if uri.starts_with("file:") {
            Url::parse(uri)
                .map_err(|e| invalid(e.to_string()))?
                .to_file_path()
                .map_err(|_| invalid("invalid file URL"))?
        } else {
            if uri.contains("://") {
                return Err(invalid(
                    "use WarcFile::new with a configured operator for this storage service",
                ));
            }
            Path::new(uri).to_path_buf()
        };
        let absolute = if path.is_absolute() {
            path
        } else {
            std::env::current_dir()?.join(path)
        };
        if absolute.file_name().is_none() {
            return Err(invalid("input has no filename"));
        }
        let root = absolute
            .ancestors()
            .last()
            .ok_or_else(|| invalid("input has no filesystem root"))?;
        let path = absolute
            .strip_prefix(root)
            .map_err(|e| invalid(e.to_string()))?
            .to_str()
            .ok_or_else(|| invalid("input path is not UTF-8"))?
            .to_owned();
        #[cfg(windows)]
        let path = path.replace('\\', "/");
        Ok(Self {
            storage: Storage::Local(root.to_path_buf()),
            path,
            source_uri: absolute.to_string_lossy().into_owned(),
        })
    }

    /// Object path within its operator.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Source label retained alongside Arrow rows.
    pub fn source_uri(&self) -> &str {
        &self.source_uri
    }

    /// Whether the object name ends in `.gz`, ignoring case.
    pub fn is_gzip(&self) -> bool {
        self.path.to_ascii_lowercase().ends_with(".gz")
    }

    /// Create a streaming reader without buffering the object in full.
    pub async fn open(&self) -> io::Result<FuturesAsyncReader> {
        let operator = match &self.storage {
            Storage::Operator(operator) => operator.clone(),
            Storage::Local(root) => Operator::new(
                opendal::services::Fs::default().root(
                    root.to_str()
                        .ok_or_else(|| invalid("input root is not UTF-8"))?,
                ),
            )?,
        };
        let reader = operator.reader(&self.path).await?;
        Ok(reader.into_futures_async_read(..).await?)
    }
}
