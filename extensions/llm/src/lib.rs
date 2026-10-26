//! The model client pinkdb's extensions share.
//!
//! One provider-neutral transcript ([`chat`]), a built-in provider table
//! ([`registry`]), two wire protocols ([`openai`], [`anthropic`]) and one
//! blocking HTTP client ([`client`]) over a hand-parsed SSE reader ([`sse`]).
//! No async runtime, no provider SDK, no new third-party crate.
//!
//! The split is what makes several providers cheap: the transcript is the same
//! data whichever provider produced it, so a session recorded against one can
//! be re-encoded for another, and adding a protocol is a request encoder plus
//! a stream decoder behind the same [`Provider`] trait.
//!
//! **There is no default provider.** A caller names one; see [`registry`] for
//! the table, the precedence and the key-isolation rule.
//!
//! Used by `extensions/agent` (wave 106b) and, next, the agent-memory
//! extension (wave 109).

pub mod anthropic;
pub mod chat;
pub mod client;
pub mod openai;
pub mod registry;
pub mod sse;

pub use chat::{Message, ToolCall, Turn, Usage};
pub use client::{Client, Config, Provider};
pub use registry::{Entry, PROVIDERS, Plan, Protocol};
