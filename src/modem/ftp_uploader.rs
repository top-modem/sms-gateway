use anyhow::{Context, Result};
use std::time::Duration;
use suppaftp::tokio::AsyncFtpStream;

#[derive(Clone, Debug)]
pub struct FtpUploadConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub timeout: Duration,
    pub passive: bool,
}

#[derive(Clone)]
pub struct FtpUploader {
    config: FtpUploadConfig,
}

impl FtpUploader {
    pub fn new(config: FtpUploadConfig) -> Self {
        Self { config }
    }

    pub async fn upload(&self, remote_name: &str, data: &[u8]) -> Result<()> {
        let config = self.config.clone();
        let remote_name = remote_name.to_string();
        let payload = data.to_vec();

        tokio::time::timeout(config.timeout, async move {
            let address = format!("{}:{}", config.host, config.port);
            let mut ftp = AsyncFtpStream::connect(address)
                .await
                .context("FTP connection failed")?;
            ftp.login(&config.username, &config.password)
                .await
                .context("FTP authentication failed")?;
            if config.passive {
                ftp.set_mode(suppaftp::types::Mode::Passive);
                ftp.set_passive_nat_workaround(true);
            }

            let mut reader = std::io::Cursor::new(payload);
            ftp.put_file(&remote_name, &mut reader)
                .await
                .context("FTP upload failed")?;
            ftp.quit().await.context("FTP close failed")?;
            Ok::<(), anyhow::Error>(())
        })
        .await
        .context("FTP upload timed out")??;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires SMS_GATEWAY_FTP_HOST, SMS_GATEWAY_FTP_USER, and SMS_GATEWAY_FTP_PASS"]
    async fn uploads_fizz_png_to_configured_server() {
        let host = std::env::var("SMS_GATEWAY_FTP_HOST").expect("FTP host is required");
        let username = std::env::var("SMS_GATEWAY_FTP_USER").expect("FTP user is required");
        let password = std::env::var("SMS_GATEWAY_FTP_PASS").expect("FTP password is required");
        let data = std::fs::read("fizz.png").expect("fizz.png must exist at the project root");
        let remote_name = format!(
            "sms_gateway_test_fizz_{}.png",
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        );

        let uploader = FtpUploader::new(FtpUploadConfig {
            host,
            port: 21,
            username,
            password,
            timeout: Duration::from_secs(30),
            passive: true,
        });

        uploader
            .upload(&remote_name, &data)
            .await
            .expect("native FTP upload of fizz.png failed");
    }
}
