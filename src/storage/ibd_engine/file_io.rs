//! Portable positioned read for IBD engine flat files (Unix `read_at`, Windows `seek_read`).

use std::fs::File;
use std::io::Result;

/// Read `buf.len()` bytes from `file` at `offset` without mutating the file cursor (where supported).
pub fn read_at(file: &File, buf: &mut [u8], offset: u64) -> Result<usize> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_at(buf, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        file.seek_read(buf, offset)
    }
    #[cfg(not(any(unix, windows)))]
    {
        use std::io::{Read, Seek, SeekFrom};
        file.seek(SeekFrom::Start(offset))?;
        file.read(buf)
    }
}
