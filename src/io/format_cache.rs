// Copyright 2018 the Tectonic Project
// Licensed under the MIT License.

#![deny(missing_docs)]

//! Code for locally caching compiled format files.

use tempfile;
use std::ffi::{OsStr};
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};

use digest::{DigestData};
use errors::{ErrorKind, Result};
use super::{InputHandle, InputOrigin, IoProvider, OpenResult};
use status::StatusBackend;

/// A local cache for compiled format files.
///
/// The format cache takes care of saving compiled format files. It uses the
/// same root cache directory as the `LocalCache` item, but is implemented
/// separately so that there is a way to save the format files associated with
/// backends that may not have their own LocalCache.
pub struct FormatCache {
    bundle_digest: DigestData,
    formats_base: PathBuf,
}


impl FormatCache {
    /// Create a new `FormatCache`.
    ///
    /// The `bundle_digest` should be the result of the `Bundle::get_digest()`
    /// call for whichever bundle is active. The `formats_base` path should be
    /// a local cache directory.
    pub fn new(bundle_digest: DigestData, formats_base: PathBuf) -> FormatCache {
        FormatCache {
            bundle_digest: bundle_digest,
            formats_base: formats_base,
        }
    }

    /// Get an on-disk path name for a given format file. This function simply
    /// produces a path that may or may not exist.
    fn path_for_format(&mut self, name: &OsStr) -> Result<PathBuf> {
        // Remove all extensions from the format name. PathBuf.file_stem() doesn't
        // do what we want since it only strips one extension, so here we go:

        let stem = match name.to_str().and_then(|s| s.splitn(2, '.').next()) {
            Some(s) => s,
            None => {
                return Err(ErrorKind::Msg(format!("incomprehensible format file name \"{}\"",
                                                  name.to_string_lossy())).into());
            }
        };

        let mut p = self.formats_base.clone();
        p.push(format!("{}-{}-{}.fmt", self.bundle_digest.to_string(), stem, ::FORMAT_SERIAL));
        Ok(p)
    }

    fn path_for_format_marker(path: &Path) -> PathBuf {
        let mut marker_name = path.file_name().map(OsStr::to_os_string).unwrap_or_default();
        marker_name.push(".ok");
        path.with_file_name(marker_name)
    }

    fn try_wait_for_format_marker(path: &Path) -> Result<bool> {
        use std::io;
        use std::thread;
        use std::time::Duration;

        if path.exists() {
            let marker = Self::path_for_format_marker(path);
            for _ in 0..20 {
                if marker.exists() {
                    return Ok(true);
                }
                thread::sleep(Duration::from_millis(100));
            };
            return Err(io::Error::new(io::ErrorKind::TimedOut, "time out waiting for format file").into());
        } else {
            Ok(false)
        }
    }
}


impl IoProvider for FormatCache {
    fn input_open_format(&mut self, name: &OsStr, _status: &mut StatusBackend) -> OpenResult<InputHandle> {
        let path = match self.path_for_format(name) {
            Ok(p) => p,
            Err(e) => return OpenResult::Err(e.into()),
        };

        match Self::try_wait_for_format_marker(&path) {
            Ok(true) => {},
            Ok(false) => return OpenResult::NotAvailable,
            Err(e) => return OpenResult::Err(e),
        };

        let f = match super::try_open_file(&path) {
            OpenResult::Ok(f) => f,
            OpenResult::NotAvailable => return OpenResult::NotAvailable,
            OpenResult::Err(e) => return OpenResult::Err(e),
        };

        OpenResult::Ok(InputHandle::new(name, BufReader::new(f), InputOrigin::Other))
    }


    fn write_format(&mut self, name: &str, data: &[u8], _status: &mut StatusBackend) -> Result<()> {
        use std::io::{ErrorKind};
        use std::fs::{File};
        use std::ffi::OsStr;

        let final_path = self.path_for_format(OsStr::new(name))?;

        let mut temp_dest = tempfile::Builder::new()
            .prefix("format_")
            .rand_bytes(6)
            .tempfile_in(&self.formats_base)?;

        temp_dest.write_all(data)?;
        
        /* another instance might has written it, detect this case and return as succeeded */
        let mut done_marker_name = final_path.file_name().map(OsStr::to_os_string).unwrap_or_default();
        done_marker_name.push(".ok");
        let done_marker_path = final_path.with_file_name(done_marker_name);
        match temp_dest.persist_noclobber(&final_path) {
            Ok(f) => {
                use std::mem;
                mem::drop(f);
                File::create(done_marker_path)?;
                Ok(())
            },
            Err(e) => {
                if e.error.kind() == ErrorKind::AlreadyExists {
                    match Self::try_wait_for_format_marker(&final_path) {
                        Ok(true) => return Ok(()),
                        Ok(false) => {},
                        Err(e) => return Err(e.into()),
                    };
                }
                Err(e.error.into())
            }
        }
    }
}
