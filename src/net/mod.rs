//! 网络接入层：epoll 服务端（单写者事件循环，可选多前端 worker）+ 阻塞式同步客户端。
//!
//! 这里是「万级连接」目标的结构性来源：一个进程、**每连接 1 个 fd + 几 KB
//! 缓冲**，而不是 PG 那样每连接 fork 一个后端进程（约 3-10 MB RSS + 一份
//! pgglobal/锁表开销）。
//!
//! | 模块 | 职责 |
//! |------|------|
//! | [`server`] | `workers = 1`（默认）的单线程事件循环本体，兼作两条路径共用的 `Conn`/`Slot` 构件 |
//! | [`multi`] | `workers > 1`：N 条前端线程 + 唯一提交线程 |
//! | [`mail`] | 两者之间那条能被 `epoll` 唤醒的信箱 |
//! | [`client`] | 同步阻塞客户端（压测与真实接入方走同一套代码） |

pub mod client;
pub mod mail;
pub mod multi;
pub mod server;

pub use client::Client;
pub use server::{serve, Report, ServerConfig};
