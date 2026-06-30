//! Output sinks: where a dump's encoded bytes go (A3, A6). Format ⟂ Sink.
//!
//! A [`Sink`] accepts whole small files ([`Sink::put`], for the JSON metadata)
//! and finished large files streamed from a local temp file
//! ([`Sink::put_file`], for geoparquet). Streaming the large file from disk keeps
//! its bytes off the memory budget (A6) and lets object_store use S3 multipart.
//!
//! Each write returns a [`FileWritten`] (SHA-256 + byte count) so the manifest
//! can record integrity without a second pass.
//!
//! Implementations:
//! * [`ObjectStoreSink`] — local dir + S3/GCS/Azure via `object_store`
//!   (`parse_url_opts`); large files use multipart.
//! * [`TarSink`] — a sequential `.tar` (default) or `.tar.zst` archive; the dump
//!   tree packed into one file. Sequential, so writes to it serialize.
//! * [`StdoutSink`] — passthrough for single-stream output (search/NDJSON).

#[cfg(feature = "cli")]
use crate::Error;
use crate::Result;
use crate::export::manifest::{Sha256Writer, sha256_hex};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
#[cfg(feature = "cli")]
use std::sync::Mutex;

/// Integrity metadata for a written file.
#[derive(Debug, Clone)]
pub struct FileWritten {
    /// Lowercase hex SHA-256 of the bytes.
    pub sha256: String,
    /// Number of bytes written.
    pub bytes: u64,
}

/// A dump output destination. Object-safe; `async` via `async_trait`-style
/// boxed futures is avoided by keeping the trait synchronous where possible —
/// object_store is async, so its sink exposes async methods on the concrete
/// type and the [`Sink`] trait offers a blocking bridge used by the planner.
///
/// The methods take a *relative path within the dump root* (e.g.
/// `collections/landsat-c2-l2/202401.parquet`).
pub trait Sink: Send + Sync {
    /// Writes a whole small file from an in-memory buffer.
    fn put(&self, rel_path: &str, bytes: &[u8]) -> Result<FileWritten>;

    /// Writes a finished large file by streaming it from a local path.
    fn put_file(&self, rel_path: &str, src: &Path) -> Result<FileWritten>;

    /// Removes a previously written file if the sink supports it (used to drop
    /// `_checkpoint.json` on success). No-op for append-only sinks (tar/stdout).
    fn remove(&self, _rel_path: &str) -> Result<()> {
        Ok(())
    }

    /// Flushes/finalizes the sink (e.g. closes a tar archive). Idempotent.
    fn finalize(&self) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Stdout sink (single-stream passthrough)
// ---------------------------------------------------------------------------

/// Writes all bytes to stdout (single-stream output; ignores `rel_path`). Useful
/// for `pgstac dump --format ndjson -` style piping.
#[derive(Debug, Default)]
pub struct StdoutSink;

impl Sink for StdoutSink {
    fn put(&self, _rel_path: &str, bytes: &[u8]) -> Result<FileWritten> {
        let mut out = std::io::stdout().lock();
        out.write_all(bytes)?;
        out.flush()?;
        Ok(FileWritten {
            sha256: sha256_hex(bytes),
            bytes: bytes.len() as u64,
        })
    }

    fn put_file(&self, _rel_path: &str, src: &Path) -> Result<FileWritten> {
        let mut f = std::fs::File::open(src)?;
        let mut out = std::io::stdout().lock();
        let mut hasher = Sha256Writer::new();
        let mut buf = vec![0u8; 1 << 16];
        loop {
            let n = f.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            out.write_all(&buf[..n])?;
        }
        out.flush()?;
        Ok(FileWritten {
            bytes: hasher.bytes(),
            sha256: hasher.finalize_hex(),
        })
    }
}

// ---------------------------------------------------------------------------
// object_store sink (local dir + S3/GCS/Azure)
// ---------------------------------------------------------------------------

#[cfg(feature = "cli")]
mod object_store_sink {
    use super::*;
    use object_store::{ObjectStore, PutPayload, WriteMultipart, path::Path as ObjPath};
    use std::sync::Arc;
    use url::Url;

    /// Streams to any `object_store` backend (local fs, S3, GCS, Azure).
    ///
    /// Construct from a base URL (`file:///abs/dir`, `s3://bucket/prefix`, …);
    /// relative dump paths are joined onto the base prefix. Large files use
    /// multipart upload.
    pub struct ObjectStoreSink {
        store: Arc<dyn ObjectStore>,
        base_prefix: ObjPath,
        runtime: tokio::runtime::Handle,
        /// Multipart chunk size (bytes).
        chunk_size: usize,
    }

    impl std::fmt::Debug for ObjectStoreSink {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("ObjectStoreSink")
                .field("base_prefix", &self.base_prefix)
                .field("chunk_size", &self.chunk_size)
                .finish()
        }
    }

    impl ObjectStoreSink {
        /// Builds a sink from a base URL and `object_store` options
        /// (region/endpoint/credentials), bound to the current tokio runtime.
        pub fn from_url_opts(
            base_url: &str,
            options: impl IntoIterator<Item = (String, String)>,
            runtime: tokio::runtime::Handle,
        ) -> Result<Self> {
            let url = Url::parse(base_url)
                .map_err(|e| Error::Export(format!("invalid sink url {base_url}: {e}")))?;
            let (store, base_prefix) = object_store::parse_url_opts(&url, options)?;
            Ok(ObjectStoreSink {
                store: Arc::from(store),
                base_prefix,
                runtime,
                chunk_size: 8 * 1024 * 1024,
            })
        }

        fn child(&self, rel_path: &str) -> ObjPath {
            let mut p = self.base_prefix.clone();
            for part in rel_path.split('/').filter(|s| !s.is_empty()) {
                p = p.child(part);
            }
            p
        }
    }

    impl Sink for ObjectStoreSink {
        fn put(&self, rel_path: &str, bytes: &[u8]) -> Result<FileWritten> {
            let location = self.child(rel_path);
            let payload = PutPayload::from(bytes.to_vec());
            let store = self.store.clone();
            let _ = self
                .runtime
                .block_on(async move { store.put(&location, payload).await })?;
            Ok(FileWritten {
                sha256: sha256_hex(bytes),
                bytes: bytes.len() as u64,
            })
        }

        fn put_file(&self, rel_path: &str, src: &Path) -> Result<FileWritten> {
            let location = self.child(rel_path);
            let store = self.store.clone();
            let chunk_size = self.chunk_size;
            let src = src.to_path_buf();
            self.runtime.block_on(async move {
                let upload = store.put_multipart(&location).await?;
                let mut writer = WriteMultipart::new_with_chunk_size(upload, chunk_size);
                let mut f = tokio::fs::File::open(&src).await?;
                let mut hasher = Sha256Writer::new();
                let mut buf = vec![0u8; chunk_size];
                use tokio::io::AsyncReadExt;
                loop {
                    let n = f.read(&mut buf).await?;
                    if n == 0 {
                        break;
                    }
                    hasher.update(&buf[..n]);
                    writer.write(&buf[..n]);
                }
                let _ = writer.finish().await?;
                Ok::<_, Error>(FileWritten {
                    bytes: hasher.bytes(),
                    sha256: hasher.finalize_hex(),
                })
            })
        }

        fn remove(&self, rel_path: &str) -> Result<()> {
            let location = self.child(rel_path);
            let store = self.store.clone();
            match self
                .runtime
                .block_on(async move { store.delete(&location).await })
            {
                Ok(()) => Ok(()),
                Err(object_store::Error::NotFound { .. }) => Ok(()),
                Err(e) => Err(e.into()),
            }
        }
    }
}

#[cfg(feature = "cli")]
pub use object_store_sink::ObjectStoreSink;

// ---------------------------------------------------------------------------
// Local directory sink (no object_store dependency; available with `export`)
// ---------------------------------------------------------------------------

/// Writes the dump tree into a local directory. Available without the `cli`
/// feature (no `object_store`); the planner uses this for `--out <dir>`.
#[derive(Debug)]
pub struct DirSink {
    root: PathBuf,
}

impl DirSink {
    /// Creates a directory sink rooted at `root` (created if missing).
    pub fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        Ok(DirSink { root })
    }

    fn target(&self, rel_path: &str) -> PathBuf {
        let mut p = self.root.clone();
        for part in rel_path.split('/').filter(|s| !s.is_empty()) {
            p.push(part);
        }
        p
    }
}

impl Sink for DirSink {
    fn put(&self, rel_path: &str, bytes: &[u8]) -> Result<FileWritten> {
        let target = self.target(rel_path);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&target, bytes)?;
        Ok(FileWritten {
            sha256: sha256_hex(bytes),
            bytes: bytes.len() as u64,
        })
    }

    fn put_file(&self, rel_path: &str, src: &Path) -> Result<FileWritten> {
        let target = self.target(rel_path);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Copy + hash in one pass.
        let mut f = std::fs::File::open(src)?;
        let mut out = std::fs::File::create(&target)?;
        let mut hasher = Sha256Writer::new();
        let mut buf = vec![0u8; 1 << 16];
        loop {
            let n = f.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            out.write_all(&buf[..n])?;
        }
        out.flush()?;
        Ok(FileWritten {
            bytes: hasher.bytes(),
            sha256: hasher.finalize_hex(),
        })
    }

    fn remove(&self, rel_path: &str) -> Result<()> {
        let target = self.target(rel_path);
        match std::fs::remove_file(&target) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

// ---------------------------------------------------------------------------
// tar sink (sequential archive; .tar default, .tar.zst optional)
// ---------------------------------------------------------------------------

#[cfg(feature = "cli")]
mod tar_sink {
    use super::*;
    use std::fs::File;

    /// Writes the dump tree into a single sequential `.tar` archive (optionally
    /// zstd-compressed). Sequential => writes serialize (the planner accepts this
    /// for the tar sink; fs/object_store sinks stay concurrent).
    pub struct TarSink {
        inner: Mutex<TarInner>,
    }

    enum TarInner {
        Plain(tar::Builder<File>),
        Zstd(tar::Builder<zstd::Encoder<'static, File>>),
        Finalized,
    }

    impl std::fmt::Debug for TarSink {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("TarSink").finish()
        }
    }

    impl TarSink {
        /// Creates a tar sink writing to `path`. `zstd` toggles outer
        /// compression (default: plain `.tar`).
        pub fn new(path: impl AsRef<Path>, zstd_compress: bool) -> Result<Self> {
            let file = File::create(path.as_ref())?;
            let inner = if zstd_compress {
                let enc = zstd::Encoder::new(file, 0)?;
                TarInner::Zstd(tar::Builder::new(enc))
            } else {
                TarInner::Plain(tar::Builder::new(file))
            };
            Ok(TarSink {
                inner: Mutex::new(inner),
            })
        }

        fn append(&self, rel_path: &str, bytes: &[u8]) -> Result<()> {
            let mut header = tar::Header::new_gnu();
            header.set_size(bytes.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            let mut guard = self
                .inner
                .lock()
                .map_err(|_| Error::Export("tar sink poisoned".into()))?;
            match &mut *guard {
                TarInner::Plain(b) => b.append_data(&mut header, rel_path, bytes)?,
                TarInner::Zstd(b) => b.append_data(&mut header, rel_path, bytes)?,
                TarInner::Finalized => {
                    return Err(Error::Export("tar sink already finalized".into()));
                }
            }
            Ok(())
        }
    }

    impl Sink for TarSink {
        fn put(&self, rel_path: &str, bytes: &[u8]) -> Result<FileWritten> {
            self.append(rel_path, bytes)?;
            Ok(FileWritten {
                sha256: sha256_hex(bytes),
                bytes: bytes.len() as u64,
            })
        }

        fn put_file(&self, rel_path: &str, src: &Path) -> Result<FileWritten> {
            // tar needs the size up front; read the (already-finished) file in.
            // Hash as we read so we don't traverse twice.
            let mut f = File::open(src)?;
            let mut bytes = Vec::new();
            let _ = f.read_to_end(&mut bytes)?;
            let written = FileWritten {
                sha256: sha256_hex(&bytes),
                bytes: bytes.len() as u64,
            };
            self.append(rel_path, &bytes)?;
            Ok(written)
        }

        fn finalize(&self) -> Result<()> {
            let mut guard = self
                .inner
                .lock()
                .map_err(|_| Error::Export("tar sink poisoned".into()))?;
            let inner = std::mem::replace(&mut *guard, TarInner::Finalized);
            match inner {
                TarInner::Plain(b) => {
                    let _ = b.into_inner()?;
                }
                TarInner::Zstd(b) => {
                    let enc = b.into_inner()?;
                    let _ = enc.finish()?;
                }
                TarInner::Finalized => {}
            }
            Ok(())
        }
    }
}

#[cfg(feature = "cli")]
pub use tar_sink::TarSink;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dir_sink_put_and_put_file() {
        let dir = tempfile::tempdir().unwrap();
        let sink = DirSink::new(dir.path()).unwrap();
        let w = sink.put("a/b/meta.json", b"{\"k\":1}").unwrap();
        assert_eq!(w.bytes, 7);
        assert_eq!(w.sha256, sha256_hex(b"{\"k\":1}"));
        let written = std::fs::read(dir.path().join("a/b/meta.json")).unwrap();
        assert_eq!(written, b"{\"k\":1}");

        // put_file streams + hashes.
        let mut tf = tempfile::NamedTempFile::new().unwrap();
        Write::write_all(&mut tf, b"parquet-bytes").unwrap();
        let w2 = sink.put_file("c/items.parquet", tf.path()).unwrap();
        assert_eq!(w2.bytes, 13);
        assert_eq!(w2.sha256, sha256_hex(b"parquet-bytes"));
        let copied = std::fs::read(dir.path().join("c/items.parquet")).unwrap();
        assert_eq!(copied, b"parquet-bytes");
    }

    #[cfg(feature = "cli")]
    #[test]
    fn tar_sink_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let tar_path = dir.path().join("dump.tar");
        {
            let sink = TarSink::new(&tar_path, false).unwrap();
            let _ = sink.put("manifest.json", b"{}").unwrap();
            let mut tf = tempfile::NamedTempFile::new().unwrap();
            Write::write_all(&mut tf, b"col").unwrap();
            let _ = sink
                .put_file("collections/c/items.parquet", tf.path())
                .unwrap();
            sink.finalize().unwrap();
        }
        // Read back the tar entries.
        let f = std::fs::File::open(&tar_path).unwrap();
        let mut ar = tar::Archive::new(f);
        let mut names = Vec::new();
        for entry in ar.entries().unwrap() {
            let entry = entry.unwrap();
            names.push(entry.path().unwrap().to_string_lossy().into_owned());
        }
        assert!(names.contains(&"manifest.json".to_string()));
        assert!(names.contains(&"collections/c/items.parquet".to_string()));
    }
}
