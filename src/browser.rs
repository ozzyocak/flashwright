use anyhow::{Context, Result, bail};
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::process::{Child, Command};

pub struct Browser {
    child: Child,
    user_data_dir: PathBuf,
    pub ws_url: String,
}

fn find_chrome(custom: Option<&str>) -> Result<PathBuf> {
    if let Some(p) = custom {
        let pb = PathBuf::from(p);
        if pb.exists() {
            return Ok(pb);
        }
        bail!("chrome not found at {}", p);
    }
    if let Ok(p) = std::env::var("CHROME_PATH") {
        let pb = PathBuf::from(&p);
        if pb.exists() {
            return Ok(pb);
        }
    }
    let candidates: &[&str] = &[
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
        "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
        "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
        "/usr/bin/google-chrome",
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
        "/usr/local/bin/chrome",
    ];
    for c in candidates {
        if std::path::Path::new(c).exists() {
            return Ok(PathBuf::from(c));
        }
    }
    bail!("no chrome found; set CHROME_PATH or pass --chrome")
}

impl Browser {
    pub async fn launch(chrome: Option<&str>, headless: bool) -> Result<Browser> {
        let exe = find_chrome(chrome)?;
        let user_data_dir = std::env::temp_dir().join(format!("flashwright-{}", std::process::id()));
        tokio::fs::create_dir_all(&user_data_dir).await?;

        let mut args: Vec<String> = vec![
            "--remote-debugging-port=0".into(),
            format!("--user-data-dir={}", user_data_dir.display()),
            "--no-first-run".into(),
            "--no-default-browser-check".into(),
            "--disable-background-networking".into(),
            "--disable-background-timer-throttling".into(),
            "--disable-backgrounding-occluded-windows".into(),
            "--disable-breakpad".into(),
            "--disable-component-extensions-with-background-pages".into(),
            "--disable-component-update".into(),
            "--disable-default-apps".into(),
            "--disable-dev-shm-usage".into(),
            "--disable-extensions".into(),
            "--disable-features=TranslateUI".into(),
            "--disable-hang-monitor".into(),
            "--disable-ipc-flooding-protection".into(),
            "--disable-popup-blocking".into(),
            "--disable-prompt-on-repost".into(),
            "--disable-renderer-backgrounding".into(),
            "--disable-sync".into(),
            "--enable-automation".into(),
            "--force-color-profile=srgb".into(),
            "--metrics-recording-only".into(),
            "--mute-audio".into(),
            "--no-sandbox".into(),
            "--password-store=basic".into(),
            "--use-mock-keychain".into(),
            "--disable-gpu".into(),
            "--disable-software-rasterizer".into(),
            "--disable-features=SiteIsolation,site-per-process,IsolateOrigins".into(),
            "--disable-blink-features=AutomationControlled".into(),
            "--no-zygote".into(),
            "--disable-web-security".into(),
            "--disable-features=BackForwardCache".into(),
            "about:blank".into(),
        ];
        if headless {
            args.insert(0, "--headless=new".into());
        }

        let mut cmd = Command::new(&exe);
        let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        cmd.args(&arg_refs)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        let mut child = cmd.spawn().context("failed to spawn chrome")?;

        let port_file = user_data_dir.join("DevToolsActivePort");
        let deadline = Instant::now() + Duration::from_secs(30);
        let ws_url;

        loop {
            if Instant::now() > deadline {
                bail!("timed out waiting for chrome DevTools endpoint");
            }
            match child.try_wait()? {
                Some(status) => bail!("chrome exited before ready: {}", status),
                None => {}
            }
            if let Ok(content) = tokio::fs::read_to_string(&port_file).await {
                let mut lines = content.lines();
                if let (Some(port), Some(path)) = (lines.next(), lines.next()) {
                    let port = port.trim();
                    let path = path.trim();
                    if !port.is_empty() && path.starts_with('/') {
                        ws_url = format!("ws://127.0.0.1:{}{}", port, path);
                        break;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        Ok(Browser { child, user_data_dir, ws_url })
    }

    pub async fn close(&mut self) {
        let _ = self.child.start_kill();
    }
}

impl Drop for Browser {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        let dir = self.user_data_dir.clone();
        std::thread::spawn(move || {
            let _ = std::fs::remove_dir_all(&dir);
        });
    }
}
