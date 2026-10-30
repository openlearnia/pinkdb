//! The console API over `fetch`: same-origin, with the session cookie, and
//! the CSRF token on every `POST`.

use serde::Serialize;
use serde::de::DeserializeOwned;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{Request, RequestCredentials, RequestInit, Response};

/// A failed call: the HTTP status (0 for a network error) and the message.
#[derive(Clone, Debug)]
pub struct ApiError {
    pub status: u16,
    pub message: String,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

fn js_error(value: JsValue) -> ApiError {
    ApiError {
        status: 0,
        message: value
            .as_string()
            .or_else(|| {
                value
                    .dyn_ref::<js_sys::Error>()
                    .map(|e| String::from(e.message()))
            })
            .unwrap_or_else(|| "network error".to_string()),
    }
}

/// Send a request and return the response, whatever its status.
pub async fn send(
    method: &str,
    path: &str,
    body: Option<String>,
    csrf: Option<&str>,
    signal: Option<&web_sys::AbortSignal>,
) -> Result<Response, ApiError> {
    let init = RequestInit::new();
    init.set_method(method);
    init.set_credentials(RequestCredentials::SameOrigin);
    if let Some(signal) = signal {
        init.set_signal(Some(signal));
    }
    let headers = web_sys::Headers::new().map_err(js_error)?;
    headers
        .set("Accept", "application/json")
        .map_err(js_error)?;
    if let Some(body) = &body {
        headers
            .set("Content-Type", "application/json")
            .map_err(js_error)?;
        init.set_body(&JsValue::from_str(body));
    }
    if let Some(csrf) = csrf {
        headers.set("X-CSRF-Token", csrf).map_err(js_error)?;
    }
    init.set_headers(&headers);
    let request = Request::new_with_str_and_init(path, &init).map_err(js_error)?;
    let window = web_sys::window().ok_or_else(|| js_error(JsValue::from_str("no window")))?;
    let response = JsFuture::from(window.fetch_with_request(&request))
        .await
        .map_err(js_error)?;
    response.dyn_into::<Response>().map_err(js_error)
}

/// The body of a response as text.
pub async fn text(response: &Response) -> Result<String, ApiError> {
    let promise = response.text().map_err(js_error)?;
    let value = JsFuture::from(promise).await.map_err(js_error)?;
    Ok(value.as_string().unwrap_or_default())
}

/// Decode a JSON answer, or turn an error body into an [`ApiError`].
async fn decode<T: DeserializeOwned>(response: Response) -> Result<T, ApiError> {
    let status = response.status();
    let body = text(&response).await?;
    if (200..300).contains(&status) {
        serde_json::from_str(&body).map_err(|e| ApiError {
            status,
            message: format!("unexpected answer from the console: {e}"),
        })
    } else {
        let message = serde_json::from_str::<pinkdb_server_ui_api::ErrorBody>(&body)
            .map(|e| e.error)
            .unwrap_or_else(|_| format!("HTTP {status}"));
        Err(ApiError { status, message })
    }
}

/// `GET path` as JSON.
pub async fn get<T: DeserializeOwned>(path: &str) -> Result<T, ApiError> {
    decode(send("GET", path, None, None, None).await?).await
}

/// `POST path` with a JSON body, as JSON.
pub async fn post<B: Serialize, T: DeserializeOwned>(
    path: &str,
    body: &B,
    csrf: &str,
) -> Result<T, ApiError> {
    let body = serde_json::to_string(body).map_err(|e| ApiError {
        status: 0,
        message: e.to_string(),
    })?;
    decode(send("POST", path, Some(body), Some(csrf), None).await?).await
}

/// `POST path` and return the raw response (downloads, event streams).
pub async fn post_raw<B: Serialize>(
    path: &str,
    body: &B,
    csrf: &str,
    signal: Option<&web_sys::AbortSignal>,
) -> Result<Response, ApiError> {
    let body = serde_json::to_string(body).map_err(|e| ApiError {
        status: 0,
        message: e.to_string(),
    })?;
    let response = send("POST", path, Some(body), Some(csrf), signal).await?;
    if response.ok() {
        Ok(response)
    } else {
        let status = response.status();
        let body = text(&response).await.unwrap_or_default();
        let message = serde_json::from_str::<pinkdb_server_ui_api::ErrorBody>(&body)
            .map(|e| e.error)
            .unwrap_or_else(|_| format!("HTTP {status}"));
        Err(ApiError { status, message })
    }
}

/// `POST path` with a file as the body (an upload), as JSON.
pub async fn post_file<T: DeserializeOwned>(
    path: &str,
    file: &web_sys::File,
    csrf: &str,
) -> Result<T, ApiError> {
    let init = RequestInit::new();
    init.set_method("POST");
    init.set_credentials(RequestCredentials::SameOrigin);
    let headers = web_sys::Headers::new().map_err(js_error)?;
    headers
        .set("Accept", "application/json")
        .map_err(js_error)?;
    headers
        .set("Content-Type", "application/octet-stream")
        .map_err(js_error)?;
    headers.set("X-CSRF-Token", csrf).map_err(js_error)?;
    init.set_headers(&headers);
    init.set_body(file);
    let request = Request::new_with_str_and_init(path, &init).map_err(js_error)?;
    let window = web_sys::window().ok_or_else(|| js_error(JsValue::from_str("no window")))?;
    let response = JsFuture::from(window.fetch_with_request(&request))
        .await
        .map_err(js_error)?;
    decode(response.dyn_into::<Response>().map_err(js_error)?).await
}

/// Read a streamed body chunk by chunk; `on_text` gets each piece of text
/// (UTF-8 sequences split across chunks are joined first).
pub async fn read_stream(
    response: &Response,
    mut on_text: impl FnMut(&str),
) -> Result<(), ApiError> {
    let body = response.body().ok_or_else(|| ApiError {
        status: 0,
        message: "the answer has no body".to_string(),
    })?;
    let reader: web_sys::ReadableStreamDefaultReader = body
        .get_reader()
        .dyn_into()
        .map_err(|o: js_sys::Object| js_error(o.into()))?;
    let mut pending: Vec<u8> = Vec::new();
    loop {
        let chunk = JsFuture::from(reader.read()).await.map_err(js_error)?;
        let done = js_sys::Reflect::get(&chunk, &JsValue::from_str("done"))
            .map(|v| v.as_bool().unwrap_or(false))
            .unwrap_or(true);
        if done {
            break;
        }
        let value = js_sys::Reflect::get(&chunk, &JsValue::from_str("value")).map_err(js_error)?;
        pending.extend(js_sys::Uint8Array::new(&value).to_vec());
        let valid = match std::str::from_utf8(&pending) {
            Ok(_) => pending.len(),
            Err(e) => e.valid_up_to(),
        };
        if valid > 0 {
            let rest = pending.split_off(valid);
            on_text(std::str::from_utf8(&pending).unwrap_or_default());
            pending = rest;
        }
    }
    Ok(())
}

/// Save `bytes` as a download named `name`.
pub fn download(name: &str, mime: &str, parts: &js_sys::Array) {
    let options = web_sys::BlobPropertyBag::new();
    options.set_type(mime);
    let Ok(blob) = web_sys::Blob::new_with_str_sequence_and_options(parts, &options) else {
        return;
    };
    download_blob(name, &blob);
}

/// Save a blob as a download named `name`.
pub fn download_blob(name: &str, blob: &web_sys::Blob) {
    let Ok(url) = web_sys::Url::create_object_url_with_blob(blob) else {
        return;
    };
    let document = leptos::prelude::document();
    if let Ok(anchor) = document.create_element("a")
        && let Ok(anchor) = anchor.dyn_into::<web_sys::HtmlAnchorElement>()
    {
        anchor.set_href(&url);
        anchor.set_download(name);
        anchor.click();
    }
    let _ = web_sys::Url::revoke_object_url(&url);
}

/// A random id for a run (not a secret: it only names the run to cancel).
pub fn run_id() -> String {
    let a = (js_sys::Math::random() * 1e15) as u64;
    let b = (js_sys::Math::random() * 1e15) as u64;
    format!("r{a:x}{b:x}")
}
