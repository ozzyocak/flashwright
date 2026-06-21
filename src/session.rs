use anyhow::{Context, Result, bail};
use base64::Engine;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

type Pending = Arc<StdMutex<HashMap<u64, oneshot::Sender<Value>>>>;
type EventSubs = Arc<StdMutex<HashMap<String, Vec<oneshot::Sender<Value>>>>>;

const WS_MASK: [u8; 4] = [0x42, 0x69, 0x6f, 0x4e];

fn encode_ws_frame(msg: &str) -> Vec<u8> {
    let payload = msg.as_bytes();
    let len = payload.len();
    let mut frame = Vec::with_capacity(len + 14);
    frame.push(0x81);
    if len <= 125 {
        frame.push(0x80 | len as u8);
    } else if len <= 65535 {
        frame.push(0x80 | 126);
        frame.push((len >> 8) as u8);
        frame.push((len & 0xff) as u8);
    } else {
        frame.push(0x80 | 127);
        frame.extend_from_slice(&[
            (len >> 56) as u8, (len >> 48) as u8, (len >> 40) as u8,
            (len >> 32) as u8, (len >> 24) as u8, (len >> 16) as u8,
            (len >> 8) as u8, len as u8,
        ]);
    }
    frame.extend_from_slice(&WS_MASK);
    for (i, &b) in payload.iter().enumerate() {
        frame.push(b ^ WS_MASK[i % 4]);
    }
    frame
}

async fn decode_ws_frame(reader: &mut OwnedReadHalf) -> Result<String> {
    let mut hdr = [0u8; 2];
    reader.read_exact(&mut hdr).await?;
    let opcode = hdr[0] & 0x0f;
    let masked = (hdr[1] & 0x80) != 0;
    let mut len = (hdr[1] & 0x7f) as usize;
    if len == 126 {
        let mut ext = [0u8; 2];
        reader.read_exact(&mut ext).await?;
        len = ((ext[0] as usize) << 8) | (ext[1] as usize);
    } else if len == 127 {
        let mut ext = [0u8; 8];
        reader.read_exact(&mut ext).await?;
        len = ((ext[0] as usize) << 56) | ((ext[1] as usize) << 48)
            | ((ext[2] as usize) << 40) | ((ext[3] as usize) << 32)
            | ((ext[4] as usize) << 24) | ((ext[5] as usize) << 16)
            | ((ext[6] as usize) << 8) | (ext[7] as usize);
    }
    let mut mask = [0u8; 4];
    if masked {
        reader.read_exact(&mut mask).await?;
    }
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).await?;
    if masked {
        for (i, b) in payload.iter_mut().enumerate() {
            *b ^= mask[i % 4];
        }
    }
    if opcode == 8 {
        bail!("websocket closed");
    }
    Ok(String::from_utf8_lossy(&payload).into_owned())
}

pub struct Session {
    next_id: AtomicU64,
    write_tx: mpsc::UnboundedSender<Vec<u8>>,
    pending: Pending,
    event_subs: EventSubs,
}

impl Session {
    pub async fn connect(url: &str) -> Result<Session> {
        let rest = url.strip_prefix("ws://").unwrap_or(url);
        let (host_port, path) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i..]),
            None => (rest, "/"),
        };

        let mut stream = TcpStream::connect(host_port).await?;

        let key = base64::engine::general_purpose::STANDARD.encode(b"flashwright-cdp!!");
        let req = format!(
            "GET {} HTTP/1.1\r\nHost: {}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {}\r\nSec-WebSocket-Version: 13\r\n\r\n",
            path, host_port, key
        );
        stream.write_all(req.as_bytes()).await?;

        let mut resp_buf = Vec::new();
        let mut byte = [0u8; 1];
        loop {
            stream.read_exact(&mut byte).await?;
            resp_buf.push(byte[0]);
            if resp_buf.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        let resp = String::from_utf8_lossy(&resp_buf);
        if !resp.contains("101") {
            bail!("websocket handshake failed");
        }

        let (read_half, write_half) = stream.into_split();
        let pending: Pending = Arc::new(StdMutex::new(HashMap::new()));
        let pending_r = pending.clone();
        let event_subs: EventSubs = Arc::new(StdMutex::new(HashMap::new()));
        let event_subs_r = event_subs.clone();
        let (write_tx, mut write_rx) = mpsc::unbounded_channel::<Vec<u8>>();

        tokio::spawn(async move {
            let mut writer = write_half;
            while let Some(frame) = write_rx.recv().await {
                if writer.write_all(&frame).await.is_err() {
                    break;
                }
            }
        });

        tokio::spawn(async move {
            let mut reader = read_half;
            loop {
                match decode_ws_frame(&mut reader).await {
                    Ok(text) => {
                        let v: Value = match serde_json::from_str(&text) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        if let Some(id) = v.get("id").and_then(|i| i.as_u64()) {
                            if let Some(sender) = pending_r.lock().unwrap().remove(&id) {
                                let _ = sender.send(v);
                            }
                        } else if let Some(method) = v.get("method").and_then(|m| m.as_str()) {
                            if let Some(handlers) = event_subs_r.lock().unwrap().get_mut(method) {
                                while let Some(sender) = handlers.pop() {
                                    let _ = sender.send(v.clone());
                                }
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        Ok(Session {
            next_id: AtomicU64::new(1),
            write_tx,
            pending,
            event_subs,
        })
    }

    pub async fn send(&self, method: &str, params: Value, session_id: Option<&str>) -> Result<Value> {
        let rx = self.send_nowait(method, params, session_id).await?;
        let resp = tokio::time::timeout(Duration::from_secs(60), rx)
            .await
            .context("command timed out")??;
        if let Some(err) = resp.get("error") {
            bail!("CDP error on {}: {}", method, err);
        }
        Ok(resp.get("result").cloned().unwrap_or(Value::Null))
    }

    pub async fn send_nowait(&self, method: &str, params: Value, session_id: Option<&str>) -> Result<oneshot::Receiver<Value>> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let mut msg = json!({ "id": id, "method": method, "params": params });
        if let Some(sid) = session_id {
            msg["sessionId"] = json!(sid);
        }
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, tx);
        let frame = encode_ws_frame(&msg.to_string());
        self.write_tx.send(frame).context("transport closed")?;
        Ok(rx)
    }

    pub async fn send_batch(&self, messages: Vec<(&str, Value)>, session_id: Option<&str>) -> Result<Vec<oneshot::Receiver<Value>>> {
        let mut receivers = Vec::with_capacity(messages.len());
        let mut buf = Vec::new();
        {
            let mut pending = self.pending.lock().unwrap();
            for (method, params) in &messages {
                let id = self.next_id.fetch_add(1, Ordering::SeqCst);
                let mut msg = json!({ "id": id, "method": *method, "params": params });
                if let Some(sid) = session_id {
                    msg["sessionId"] = json!(sid);
                }
                buf.extend_from_slice(&encode_ws_frame(&msg.to_string()));
                let (tx, rx) = oneshot::channel();
                pending.insert(id, tx);
                receivers.push(rx);
            }
        }
        self.write_tx.send(buf).context("transport closed")?;
        Ok(receivers)
    }

    pub async fn await_all(receivers: Vec<oneshot::Receiver<Value>>) -> Result<Vec<Value>> {
        let mut results = Vec::with_capacity(receivers.len());
        for rx in receivers {
            let resp = tokio::time::timeout(Duration::from_secs(10), rx)
                .await
                .context("batch command timed out")??;
            if let Some(err) = resp.get("error") {
                bail!("CDP batch error: {}", err);
            }
            results.push(resp.get("result").cloned().unwrap_or(Value::Null));
        }
        Ok(results)
    }

    pub fn subscribe_event(&self, method: &str) -> oneshot::Receiver<Value> {
        let (tx, rx) = oneshot::channel();
        self.event_subs
            .lock()
            .unwrap()
            .entry(method.to_string())
            .or_insert_with(Vec::new)
            .push(tx);
        rx
    }
}

pub struct Page {
    pub session_id: String,
}

impl Page {
    pub async fn enable(&self, s: &Session) -> Result<()> {
        let receivers = s.send_batch(
            vec![("Page.enable", json!({})), ("Runtime.enable", json!({}))],
            Some(&self.session_id),
        ).await?;
        Session::await_all(receivers).await?;
        Ok(())
    }

    pub async fn set_viewport(&self, s: &Session, w: u32, h: u32) -> Result<()> {
        s.send(
            "Emulation.setDeviceMetricsOverride",
            json!({ "width": w, "height": h, "deviceScaleFactor": 1, "mobile": false }),
            Some(&self.session_id),
        ).await?;
        Ok(())
    }

    pub async fn navigate(&self, s: &Session, url: &str) -> Result<()> {
        let event_rx = s.subscribe_event("Page.domContentEventFired");
        s.send("Page.navigate", json!({"url": url}), Some(&self.session_id)).await?;
        match tokio::time::timeout(Duration::from_secs(10), event_rx).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(_)) => Ok(()),
            Err(_) => Ok(()),
        }
    }

    pub async fn eval(&self, s: &Session, expr: &str) -> Result<Value> {
        let resp = s.send(
            "Runtime.evaluate",
            json!({ "expression": expr, "returnByValue": true, "awaitPromise": true, "allowUnsafeEval": true }),
            Some(&self.session_id),
        ).await?;
        if let Some(exc) = resp.get("exceptionDetails") {
            bail!("eval error: {}", exc);
        }
        let result = resp.get("result").context("no eval result")?;
        Ok(result.get("value").cloned().unwrap_or(Value::Null))
    }

    pub async fn click(&self, s: &Session, selector: &str) -> Result<()> {
        let pos = self.get_pos(s, selector).await?;
        let sid = &self.session_id;
        let receivers = s.send_batch(
            vec![
                ("Input.dispatchMouseEvent", json!({"type":"mouseMoved","x":pos.0,"y":pos.1})),
                ("Input.dispatchMouseEvent", json!({"type":"mousePressed","x":pos.0,"y":pos.1,"button":"left","clickCount":1})),
                ("Input.dispatchMouseEvent", json!({"type":"mouseReleased","x":pos.0,"y":pos.1,"button":"left","clickCount":1})),
            ],
            Some(sid),
        ).await?;
        Session::await_all(receivers).await?;
        Ok(())
    }

    pub async fn type_text(&self, s: &Session, selector: &str, text: &str) -> Result<()> {
        let pos = self.get_pos(s, selector).await?;
        let sid = &self.session_id;
        let receivers = s.send_batch(
            vec![
                ("Input.dispatchMouseEvent", json!({"type":"mouseMoved","x":pos.0,"y":pos.1})),
                ("Input.dispatchMouseEvent", json!({"type":"mousePressed","x":pos.0,"y":pos.1,"button":"left","clickCount":1})),
                ("Input.dispatchMouseEvent", json!({"type":"mouseReleased","x":pos.0,"y":pos.1,"button":"left","clickCount":1})),
                ("Input.insertText", json!({"text": text})),
            ],
            Some(sid),
        ).await?;
        Session::await_all(receivers).await?;
        Ok(())
    }

    async fn get_pos(&self, s: &Session, selector: &str) -> Result<(f64, f64)> {
        let expr = format!(
            r#"(() => {{ const el = document.querySelector({sel:?}); if(!el) return null; el.scrollIntoView({{block:'center',inline:'center'}}); const r = el.getBoundingClientRect(); return [r.x + r.width/2, r.y + r.height/2]; }})()"#,
            sel = selector
        );
        let v = self.eval(s, &expr).await?;
        let arr = v.as_array().context("element not found or not visible")?;
        Ok((arr[0].as_f64().unwrap_or(0.0), arr[1].as_f64().unwrap_or(0.0)))
    }

    pub async fn screenshot(&self, s: &Session, format: &str) -> Result<Vec<u8>> {
        let mut params = json!({"format": format, "captureBeyondViewport": false});
        if format == "jpeg" {
            params["quality"] = json!(80);
        }
        let resp = s.send("Page.captureScreenshot", params, Some(&self.session_id)).await?;
        let b64 = resp.get("data").and_then(|v| v.as_str()).context("no screenshot data")?;
        Ok(base64::engine::general_purpose::STANDARD.decode(b64)?)
    }

    pub async fn pdf(&self, s: &Session) -> Result<Vec<u8>> {
        let resp = s.send("Page.printToPDF", json!({"printBackground": true}), Some(&self.session_id)).await?;
        let b64 = resp.get("data").and_then(|v| v.as_str()).context("no pdf data")?;
        Ok(base64::engine::general_purpose::STANDARD.decode(b64)?)
    }

    pub async fn wait_selector(&self, s: &Session, selector: &str, timeout_ms: u64) -> Result<()> {
        let expr = format!(
            r#"new Promise((resolve) => {{ if(document.querySelector({sel:?})) return resolve(true); const obs = new MutationObserver(() => {{ if(document.querySelector({sel:?})) {{ obs.disconnect(); resolve(true); }} }}); obs.observe(document.documentElement || document.body, {{childList:true, subtree:true}}); setTimeout(() => {{ obs.disconnect(); resolve(false); }}, {timeout}); }})"#,
            sel = selector, timeout = timeout_ms
        );
        let v = self.eval(s, &expr).await?;
        if v == json!(false) {
            bail!("timeout waiting for selector {}", selector);
        }
        Ok(())
    }
}
