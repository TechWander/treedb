//! 网络接入层：epoll 服务端（单写者事件循环，可选多前端 worker）+ 阻塞式同步客户端。
//!
//! 这里是「万级连接」目标的结构性来源：一个进程、**每连接 1 个 fd + 几 KB
//! 缓冲**，而不是 PG 那样每连接 fork 一个后端进程（约 3-10 MB RSS + 一份
//! pgglobal/锁表开销）。
//!
//! | 模块 | 职责 |
//! |------|------|
//! | [`server`] | `workers = 1` 的单线程事件循环本体，兼作两条路径共用的 `Conn`/`Slot` 构件 |
//! | [`multi`] | `workers > 1`：N 条前端线程 + 唯一提交线程 |
//! | [`mail`] | 两者之间那条能被 `epoll` 唤醒的信箱 |
//! | [`client`] | 同步阻塞客户端（压测与真实接入方走同一套代码） |
//! | [`replicate`] | 只读副本的追平循环（拉主，不是主推） |

pub mod client;
pub mod mail;
pub mod multi;
pub mod replicate;
pub mod server;

pub use client::Client;
pub use replicate::{Follower, ReplicaCfg};
pub use server::{serve, Report, ServerConfig};

/// 默认前端 worker 线程数（`ServerConfig::new` 与 `--workers` 不显式给值时走这里）。
///
/// 为什么默认不是 1：读路径是一个核一条循环，而 PG 是每连接一个后端进程、
/// 天然吃满多核。同机同负载实测（`benches/throughput_matrix.sh` 轴 5）里
/// `workers = 1` 的高并发纯读格子全面落后 PG，而 workers 一升到 8/16 就整体
/// 反超。把一个“默认跑分就输在核数上”的值当默认值，是对使用者不负责。
///
/// 取 `核数 / 2`、封顶 8：
/// - 除以 2：除 worker 外还有一条提交线程与客户端进程要核，1:1 超线程式抢核
///   会在写档上回吐收益；
/// - 封顶 8：实测读吞吐从 8 到 16 只再涨 1.5-1.9×（不到线性），再多就是
///   白付线程与 fd（每个 worker +3 fd）；
/// - 1-3 核机器上结果就是 1，行为与旧默认逐字一致（单线程那条循环代码路径
///   根本没被路过）。
pub fn default_workers() -> usize {
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    (cores / 2).clamp(1, 8)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 默认值必须落在「1..=8」这段实测有收益的区间里，且不能因为
    /// `available_parallelism()` 失败就退化成 0（0 会去走单线程分支，
    /// 但 `rep.workers` 与日志里的数字就对不上了）。
    #[test]
    fn default_workers_is_bounded_and_never_zero() {
        let w = default_workers();
        assert!((1..=8).contains(&w), "default_workers() = {w}");
        let cfg = ServerConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            std::env::temp_dir().join("treedb-dw-test"),
        );
        assert_eq!(cfg.workers, w, "ServerConfig::new 必须把 auto 解成实际线程数");
    }
}
