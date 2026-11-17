use anyhow::{Context, Result, ensure};
use std::{fs, path::Path};

pub fn read_token(path: &Path) -> Result<String> {
    let token = fs::read_to_string(path)
        .context("Read service token")?
        .trim()
        .to_owned();
    ensure!(
        token.len() >= 32 && token.is_ascii(),
        "Invalid service token"
    );
    Ok(token)
}
pub fn token(home: &Path) -> Result<String> {
    read_token(&home.join("token"))
}

/// Create a private file without ever overwriting an existing secret/config/unit.
pub fn private_write(path: &Path, data: &[u8]) -> Result<()> {
    use std::{io::Write, os::unix::fs::OpenOptionsExt};
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(data)?;
    file.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    #[test]
    fn private_files_are_not_overwritten() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("token");
        private_write(&path, b"0123456789abcdef0123456789abcdef\n")?;
        assert_eq!(fs::metadata(&path)?.permissions().mode() & 0o777, 0o600);
        assert_eq!(read_token(&path)?, "0123456789abcdef0123456789abcdef");
        assert!(private_write(&path, b"overwrite").is_err());
        Ok(())
    }
}
