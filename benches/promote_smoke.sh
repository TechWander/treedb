#!/bin/bash
# 切主的活体验证（不进回归板）：真二进制、真端口、真目录。
# ① 主 A → 副本 B 追平；② B 被占着时 --promote 必须被拒；③ 停 B 后提升；
# ④ 提升出来的目录能当主（数据在、写能进）；⑤ 旧主 A 回魂后，令牌更高的副本拒收它。
set -u
R=$(cd "$(dirname "$0")/.." && pwd)/target/release
D=/tmp/fence_smoke
rm -rf "$D"; mkdir -p "$D/A"
say() { printf '\n=== %s\n' "$*"; }
st() { "$R/treedb-server" --listen "127.0.0.1:$1" --stats 2>&1 | grep -E "nodes|seq|proto_version|replica" ; }

say "1. 起主 A(:7101)，写一点"
"$R/treedb-server" --dir "$D/A" --listen 127.0.0.1:7101 --pid-file "$D/a.pid" >"$D/a.log" 2>&1 &
sleep 0.6
"$R/treedb-bench" --addr 127.0.0.1:7101 --op create --conns 2 --threads 2 --views 4 --secs 1 >"$D/benchA1.log" 2>&1
st 7101 | sed 's/^/A: /'

say "2. 起副本 B(:7102) 追 A，等它收敛"
"$R/treedb-standby" --primary 127.0.0.1:7101 --dir "$D/B" --listen 127.0.0.1:7102 \
    --pid-file "$D/b.pid" >"$D/b.log" 2>&1 &
sleep 2.5
st 7102 | sed 's/^/B: /'
grep -o "fence=[0-9]*" "$D/b.log" | head -1 | sed 's/^/B banner: /'

say "3. B 还活着时 --promote 必须被拒（目录锁）"
"$R/treedb-standby" --dir "$D/B" --promote >"$D/promote_locked.out" 2>&1
echo "exit=$?"; cat "$D/promote_locked.out"

say "4. 停 B，再提升；cat fence"
kill "$(cat "$D/b.pid")"; sleep 0.8
"$R/treedb-standby" --dir "$D/B" --promote >"$D/promote.out" 2>&1
echo "exit=$?"; cat "$D/promote.out"
echo "fence file: $(cat "$D/B/fence")"

say "5. 冷拷一份 C（fence 也是 1），留给第 7 步"
cp -a "$D/B" "$D/C"

say "6. 用提升出来的 B 当主(:7103)：数据在不在、写进不进"
"$R/treedb-server" --dir "$D/B" --listen 127.0.0.1:7103 --pid-file "$D/b2.pid" >"$D/b2.log" 2>&1 &
sleep 0.6
st 7103 | sed 's/^/B(new primary): /'
"$R/treedb-bench" --addr 127.0.0.1:7103 --op create --conns 2 --threads 2 --views 4 --secs 1 >"$D/benchB.log" 2>&1
echo "bench on promoted B: $(tail -3 "$D/benchB.log" | tr '\n' ' ')"
grep -q "EROFS" "$D/benchB.log" && echo "!! 提升后的目录仍然拒写" || echo "写进去了（提升生效）"

say "7. 旧主 A 回魂(:7101) 继续收写；让令牌为 1 的 C 去追它"
"$R/treedb-bench" --addr 127.0.0.1:7101 --op create --conns 2 --threads 2 --views 4 --secs 1 >"$D/benchA2.log" 2>&1
st 7101 | sed 's/^/A(zombie): /'
"$R/treedb-standby" --primary 127.0.0.1:7101 --dir "$D/C" --listen 127.0.0.1:7104 \
    --pid-file "$D/c.pid" >"$D/c.log" 2>&1 &
sleep 2.5
st 7104 | sed 's/^/C: /'
grep -o "is fenced out.*" "$D/c.log" | head -2 | sed 's/^/C log: /'
echo "C 的 fence 文件: $(cat "$D/C/fence")"

say "清理"
for p in a.pid b2.pid c.pid; do [ -f "$D/$p" ] && kill "$(cat "$D/$p")" 2>/dev/null; done
sleep 0.5

# 期望的判据（写进 DESIGN §6.6「切主」的活体验证里，2026-11-21 实测）：
# ① B 与 A 的 nodes/seq 相等、lag 0、banner 里 fence=0；
# ② 被占着时 --promote 退出码 1，错误里带持锁 pid；
# ③ 停服后 --promote 打印 `fence 0 -> 1`，`cat fence` 是 1；
# ④ 拿 B 起服：nodes/seq 与提升前一致，create 跑得动、errors=0；
# ⑤ 旧主 A 回魂后 nodes 继续涨，而令牌为 1 的 C 的 nodes/seq 一格不动、
#    `replica_errors` 与 lag 一起涨、日志里是 `is fenced out`。
