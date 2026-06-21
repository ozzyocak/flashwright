mod browser;
mod session;

use anyhow::{bail, Result};
use base64::Engine;
use serde_json::{json, Value};
use session::{Page, Session};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

const SOCK_PATH: &str = "/tmp/flashwrightd.sock";

struct Args {
    chrome: Option<String>,
    headed: bool,
    timeout: u64,
    viewport: Option<(u32, u32)>,
    command: String,
    cmd_args: Vec<String>,
}

fn parse_args() -> Result<Args> {
    let argv: Vec<String> = std::env::args().collect();
    let mut chrome = None;
    let mut headed = false;
    let mut timeout: u64 = 30000;
    let mut viewport: Option<(u32, u32)> = None;
    let mut positional: Vec<String> = Vec::new();

    let mut i = 1;
    while i < argv.len() {
        match argv[i].as_str() {
            "--chrome" => { i += 1; if i < argv.len() { chrome = Some(argv[i].clone()); } }
            "--headed" => headed = true,
            "--timeout" => { i += 1; if i < argv.len() { timeout = argv[i].parse().unwrap_or(30000); } }
            "--viewport" => { i += 1; if i < argv.len() { viewport = parse_viewport(&argv[i]); } }
            "--help" | "-h" => { print_help(); std::process::exit(0); }
            "--version" | "-V" => { println!("flashwright 1.0.0"); std::process::exit(0); }
            _ => positional.push(argv[i].clone()),
        }
        i += 1;
    }

    if positional.is_empty() {
        print_help();
        bail!("no command specified");
    }

    let command = positional.remove(0);
    Ok(Args { chrome, headed, timeout, viewport, command, cmd_args: positional })
}

fn print_help() {
    eprintln!(
"flashwright 1.0.0 - fast Chromium automation

USAGE:
  flashwright [OPTIONS] <COMMAND> [ARGS]

OPTIONS:
  --chrome <path>   Path to Chrome executable
  --headed          Show browser window
  --timeout <ms>    Wait timeout (default 30000)
  --viewport <WxH>  Set viewport size

COMMANDS:
  serve                    Start persistent daemon
  navigate <url>           Go to URL
  eval <expr>              Evaluate JS, print result
  click <selector>         Click element
  type <selector> <text>   Type text into field
  screenshot [file] [fmt]  Capture screenshot (png or jpeg)
  pdf [file]               Print to PDF
  wait <selector>          Wait for element
  title                    Print page title
  content                  Print page HTML
  script <file>            Run script file
  stop                     Stop the daemon

The first command auto-starts the daemon. Subsequent commands reuse it."
    );
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;

    match args.command.as_str() {
        "serve" => run_daemon(args).await,
        "stop" => {
            let mut sock = UnixStream::connect(SOCK_PATH).await?;
            write_json_line(&mut sock, &json!({"cmd": "stop"})).await?;
            println!("daemon stopped");
            Ok(())
        }
        _ => run_client(&args).await,
    }
}

async fn daemon_running() -> bool {
    UnixStream::connect(SOCK_PATH).await.is_ok()
}

async fn ensure_daemon(args: &Args) -> Result<()> {
    if daemon_running().await {
        return Ok(());
    }

    let exe = std::env::current_exe()?;
    let mut cmd = tokio::process::Command::new(&exe);
    cmd.arg("serve");
    if args.headed {
        cmd.arg("--headed");
    }
    if let Some(ref c) = args.chrome {
        cmd.arg("--chrome").arg(c);
    }
    cmd.stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        unsafe { cmd.pre_exec(|| { libc::setsid(); Ok(()) }); }
    }

    cmd.spawn()?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if daemon_running().await {
            return Ok(());
        }
        if tokio::time::Instant::now() > deadline {
            bail!("daemon failed to start");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn run_client(args: &Args) -> Result<()> {
    ensure_daemon(args).await?;
    let mut sock = UnixStream::connect(SOCK_PATH).await?;

    if args.command == "script" {
        let file = args.cmd_args.first().cloned().unwrap_or_default();
        let content = std::fs::read_to_string(&file)?;
        let lines: Vec<String> = content
            .lines()
            .map(|l| l.trim())
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .map(|l| l.to_string())
            .collect();

        let req = json!({"cmd": "script", "lines": lines, "timeout": args.timeout});
        write_json_line(&mut sock, &req).await?;
        let resp = read_json_line(&mut sock).await?;
        if let Some(err) = resp.get("error") {
            bail!("{}", err);
        }
        if let Some(outputs) = resp.get("outputs").and_then(|v| v.as_array()) {
            for o in outputs {
                if let Some(s) = o.as_str() {
                    println!("{}", s);
                }
            }
        }
        return Ok(());
    }

    let req = json!({
        "cmd": args.command,
        "args": args.cmd_args,
        "timeout": args.timeout,
        "viewport": args.viewport.map(|(w,h)| json!([w, h])),
    });
    write_json_line(&mut sock, &req).await?;
    let resp = read_json_line(&mut sock).await?;
    if let Some(err) = resp.get("error") {
        bail!("{}", err);
    }

    if let Some(data_b64) = resp.get("data_b64").and_then(|v| v.as_str()) {
        let bytes = base64::engine::general_purpose::STANDARD.decode(data_b64)?;
        let out = resp.get("out").and_then(|v| v.as_str())
            .filter(|s| !s.is_empty()).map(|s| s.to_string());
        write_bytes(&out, &bytes, resp.get("kind").and_then(|v| v.as_str()).unwrap_or("output"))?;
    } else if let Some(text) = resp.get("text").and_then(|v| v.as_str()) {
        println!("{}", text);
    }
    Ok(())
}

async fn run_daemon(args: Args) -> Result<()> {
    let _ = std::fs::remove_file(SOCK_PATH);

    let mut browser = browser::Browser::launch(args.chrome.as_deref(), !args.headed).await?;
    let session = Session::connect(&browser.ws_url).await?;

    let r = session.send("Target.createTarget", json!({"url":"about:blank"}), None).await?;
    let target_id = r.get("targetId").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let r = session.send("Target.attachToTarget", json!({"targetId": target_id, "flatten": true}), None).await?;
    let session_id = r.get("sessionId").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let page = Page { session_id };
    page.enable(&session).await?;

    if let Some((w, h)) = args.viewport {
        page.set_viewport(&session, w, h).await?;
    }

    let listener = UnixListener::bind(SOCK_PATH)?;
    let session = std::sync::Arc::new(session);
    let page = std::sync::Arc::new(page);

    loop {
        let (sock, _) = match listener.accept().await {
            Ok(v) => v,
            Err(_) => continue,
        };
        let session = session.clone();
        let page = page.clone();
        tokio::spawn(async move {
            handle_client(sock, session, page).await;
        });
    }
}

async fn handle_client(mut sock: UnixStream, session: std::sync::Arc<Session>, page: std::sync::Arc<Page>) {
    let req = match read_json_line(&mut sock).await {
        Ok(v) => v,
        Err(_) => return,
    };

    let cmd = req.get("cmd").and_then(|v| v.as_str()).unwrap_or("");
    let timeout = req.get("timeout").and_then(|v| v.as_u64()).unwrap_or(30000);
    let args_arr = req.get("args").and_then(|v| v.as_array());

    let result: Result<Value> = async {
        match cmd {
            "stop" => {
                let _ = std::fs::remove_file(SOCK_PATH);
                write_json_line(&mut sock, &json!({"ok": true})).await.ok();
                std::process::exit(0);
            }
            "navigate" => {
                let url = get_arg(args_arr, 0);
                page.navigate(&session, &url).await?;
                Ok(json!({"text": format!("navigated: {}", url)}))
            }
            "eval" => {
                let expr = args_arr
                    .map(|a| a.iter().map(|v| v.as_str().unwrap_or("")).collect::<Vec<_>>().join(" "))
                    .unwrap_or_default();
                let v = page.eval(&session, &expr).await?;
                Ok(json!({"text": serde_json::to_string(&v)?}))
            }
            "click" => {
                let sel = get_arg(args_arr, 0);
                page.click(&session, &sel).await?;
                Ok(json!({"text": format!("clicked: {}", sel)}))
            }
            "type" => {
                let sel = get_arg(args_arr, 0);
                let text = get_arg(args_arr, 1);
                page.type_text(&session, &sel, &text).await?;
                Ok(json!({"text": format!("typed into: {}", sel)}))
            }
            "screenshot" => {
                let out = args_arr.and_then(|a| a.get(0).and_then(|v| v.as_str())).map(|s| s.to_string());
                let format = args_arr.and_then(|a| a.get(1).and_then(|v| v.as_str())).unwrap_or("png").to_string();
                let bytes = page.screenshot(&session, &format).await?;
                let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
                Ok(json!({"data_b64": b64, "out": out.unwrap_or_default(), "kind": "screenshot"}))
            }
            "pdf" => {
                let out = args_arr.and_then(|a| a.get(0).and_then(|v| v.as_str())).map(|s| s.to_string());
                let bytes = page.pdf(&session).await?;
                let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
                Ok(json!({"data_b64": b64, "out": out.unwrap_or_default(), "kind": "page"}))
            }
            "wait" => {
                let sel = get_arg(args_arr, 0);
                page.wait_selector(&session, &sel, timeout).await?;
                Ok(json!({"text": format!("ready: {}", sel)}))
            }
            "title" => {
                let v = page.eval(&session, "document.title").await?;
                Ok(json!({"text": v.as_str().unwrap_or("")}))
            }
            "content" => {
                let v = page.eval(&session, "document.documentElement.outerHTML").await?;
                Ok(json!({"text": v.as_str().unwrap_or("")}))
            }
            "viewport" => {
                let vp = req.get("viewport").and_then(|v| v.as_array());
                if let Some(vp) = vp {
                    let w = vp[0].as_u64().unwrap_or(1280) as u32;
                    let h = vp[1].as_u64().unwrap_or(720) as u32;
                    page.set_viewport(&session, w, h).await?;
                    Ok(json!({"text": format!("viewport: {}x{}", w, h)}))
                } else {
                    bail!("no viewport specified");
                }
            }
            "script" => {
                let lines = req.get("lines").and_then(|v| v.as_array())
                    .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect::<Vec<_>>())
                    .unwrap_or_default();
                let mut outputs = Vec::new();
                for line in &lines {
                    let action = parse_script_line(line)?;
                    let out = run_action(&session, &page, action, timeout).await?;
                    if let Some(o) = out {
                        outputs.push(o);
                    }
                }
                Ok(json!({"outputs": outputs}))
            }
            other => bail!("unknown command: {}", other),
        }
    }.await;

    match result {
        Ok(v) => { let _ = write_json_line(&mut sock, &v).await; }
        Err(e) => { let _ = write_json_line(&mut sock, &json!({"error": e.to_string()})).await; }
    }
}

fn get_arg(arr: Option<&Vec<Value>>, i: usize) -> String {
    arr.and_then(|a| a.get(i).and_then(|v| v.as_str())).unwrap_or("").to_string()
}

async fn run_action(s: &Session, p: &Page, a: Action, timeout: u64) -> Result<Option<String>> {
    Ok(Some(match a {
        Action::Navigate(url) => { p.navigate(s, &url).await?; format!("navigated: {}", url) }
        Action::Eval(expr) => { let v = p.eval(s, &expr).await?; serde_json::to_string(&v)? }
        Action::Click(sel) => { p.click(s, &sel).await?; format!("clicked: {}", sel) }
        Action::Type(sel, text) => { p.type_text(s, &sel, &text).await?; format!("typed into: {}", sel) }
        Action::Screenshot(out, format) => {
            let bytes = p.screenshot(s, &format).await?;
            write_bytes(&out, &bytes, "screenshot")?;
            return Ok(None);
        }
        Action::Pdf(out) => {
            let bytes = p.pdf(s).await?;
            write_bytes(&out, &bytes, "page")?;
            return Ok(None);
        }
        Action::Wait(sel) => { p.wait_selector(s, &sel, timeout).await?; format!("ready: {}", sel) }
        Action::Title => { let v = p.eval(s, "document.title").await?; v.as_str().unwrap_or("").to_string() }
        Action::Content => { let v = p.eval(s, "document.documentElement.outerHTML").await?; v.as_str().unwrap_or("").to_string() }
        Action::Viewport(w, h) => { p.set_viewport(s, w, h).await?; format!("viewport: {}x{}", w, h) }
    }))
}

async fn write_json_line(sock: &mut UnixStream, v: &Value) -> Result<()> {
    let mut buf = serde_json::to_vec(v)?;
    buf.push(b'\n');
    sock.write_all(&buf).await?;
    Ok(())
}

async fn read_json_line(sock: &mut UnixStream) -> Result<Value> {
    let mut reader = BufReader::new(sock);
    let mut buf = String::new();
    reader.read_line(&mut buf).await?;
    Ok(serde_json::from_str(&buf)?)
}

fn write_bytes(out: &Option<String>, bytes: &[u8], kind: &str) -> Result<()> {
    match out {
        Some(path) if !path.is_empty() => {
            std::fs::write(path, bytes)?;
            println!("{} saved: {}", kind, path);
        }
        _ => {
            use std::io::Write;
            let stdout = std::io::stdout();
            let mut lock = stdout.lock();
            lock.write_all(bytes)?;
            let _ = lock.flush();
        }
    }
    Ok(())
}

fn parse_viewport(s: &str) -> Option<(u32, u32)> {
    let mut it = s.split(|c| c == 'x' || c == 'X');
    let w = it.next()?.parse().ok()?;
    let h = it.next()?.parse().ok()?;
    Some((w, h))
}

enum Action {
    Navigate(String),
    Eval(String),
    Click(String),
    Type(String, String),
    Screenshot(Option<String>, String),
    Pdf(Option<String>),
    Wait(String),
    Title,
    Content,
    Viewport(u32, u32),
}

fn parse_script_line(line: &str) -> Result<Action> {
    let tokens = tokenize(line)?;
    if tokens.is_empty() { bail!("empty line"); }
    let cmd = tokens[0].as_str();
    let args = &tokens[1..];
    match cmd {
        "navigate" | "goto" => { need(args, 1)?; Ok(Action::Navigate(args[0].clone())) }
        "eval" => { need(args, 1)?; Ok(Action::Eval(args[0].clone())) }
        "click" => { need(args, 1)?; Ok(Action::Click(args[0].clone())) }
        "type" => { need(args, 2)?; Ok(Action::Type(args[0].clone(), args[1].clone())) }
        "screenshot" => {
            let out = args.get(0).cloned();
            let format = args.get(1).cloned().unwrap_or_else(|| "png".into());
            Ok(Action::Screenshot(out, format))
        }
        "pdf" => { let out = args.get(0).cloned(); Ok(Action::Pdf(out)) }
        "wait" => { need(args, 1)?; Ok(Action::Wait(args[0].clone())) }
        "title" => Ok(Action::Title),
        "content" => Ok(Action::Content),
        "viewport" => {
            need(args, 1)?;
            let (w, h) = parse_viewport(&args[0]).ok_or_else(|| anyhow::anyhow!("expected WxH"))?;
            Ok(Action::Viewport(w, h))
        }
        other => bail!("unknown command: {}", other),
    }
}

fn need(args: &[String], n: usize) -> Result<()> {
    if args.len() < n { bail!("expected {} argument(s)", n); }
    Ok(())
}

fn tokenize(line: &str) -> Result<Vec<String>> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' | '\'' => {
                let quote = c;
                while let Some(c2) = chars.next() {
                    if c2 == '\\' {
                        if let Some(esc) = chars.next() { cur.push(esc); }
                    } else if c2 == quote {
                        break;
                    } else {
                        cur.push(c2);
                    }
                }
                out.push(std::mem::take(&mut cur));
            }
            ' ' | '\t' => {
                if !cur.is_empty() { out.push(std::mem::take(&mut cur)); }
            }
            _ => cur.push(c),
        }
    }
    if !cur.is_empty() { out.push(cur); }
    Ok(out)
}
