#!/bin/bash
# 切主的活体验证：真二进制、真端口、真目录、硬断言（DESIGN.md §6.6）。
#
# 顺序刻意按运维动作来：主 A → 副本 B 追平 → 证明 B 只读 → 被锁时拒提升 →
# 停旧主 A → 停副本 B → 离线提升 B → 冷拷同令牌的 C → B 起新主并可写 →
# 旧主 A 回魂继续写 → C 拒收旧主且状态/令牌不回退。
#
# 用法：bash benches/promote_smoke.sh
# 前置：cargo build --release --bin treedb-{server,standby,bench}
# 输出：追加 benches/results/promote_smoke.log；现场目录留在 .live/promote.*（已 gitignore）。
set -u
set -o pipefail
cd "$(dirname "$0")/.." || exit 1
ROOT=$(pwd)
R=$ROOT/target/release
OUT=benches/results/promote_smoke.log
SECS=${SMOKE_SECS:-1}
mkdir -p "$ROOT/.live" benches/results
D=$(mktemp -d "$ROOT/.live/promote.XXXXXX") || exit 1
PIDS=()

for b in treedb-server treedb-standby treedb-bench; do
    [ -x "$R/$b" ] || { echo "缺少 $R/$b，先 cargo build --release"; exit 2; }
done
case "$SECS" in ''|*[!0-9]*) echo "SMOKE_SECS 必须是正整数"; exit 2;; esac
[ "$SECS" -gt 0 ] || { echo "SMOKE_SECS 必须大于 0"; exit 2; }

# 四个端口一次占住再释放，避免逐个 newport 时撞到同一个刚释放的端口。
mapfile -t PORTS < <(python3 - <<'PY'
import socket
ss = []
for _ in range(4):
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    ss.append(s)
for s in ss:
    print(s.getsockname()[1])
PY
)
[ "${#PORTS[@]}" -eq 4 ] || { echo "分配不到四个端口"; exit 2; }
P1=127.0.0.1:${PORTS[0]}
P2=127.0.0.1:${PORTS[1]}
P3=127.0.0.1:${PORTS[2]}
P4=127.0.0.1:${PORTS[3]}

forget_pid() {
    local p=$1 keep=() x
    for x in "${PIDS[@]}"; do [ "$x" = "$p" ] || keep+=("$x"); done
    PIDS=("${keep[@]}")
}
cleanup() {
    local p
    for p in "${PIDS[@]}"; do kill -TERM "$p" 2>/dev/null || true; done
    for p in "${PIDS[@]}"; do wait "$p" 2>/dev/null || true; done
}
trap cleanup EXIT

say() { printf '\n=== %s\n' "$*"; }
fail() { printf '  FAIL: %s\n' "$*"; return 1; }
assert_eq() {
    if [ "$1" = "$2" ]; then printf '  OK: %s = %s\n' "$3" "$1";
    else fail "$3: 期望 $2，实得 $1"; fi
}
assert_gt() {
    if [ "${1:-0}" -gt "$2" ]; then printf '  OK: %s = %s > %s\n' "$3" "$1" "$2";
    else fail "$3: 期望 > $2，实得 ${1:-缺失}"; fi
}
stats() { timeout 5 "$R/treedb-server" --listen "$1" --stats 2>/dev/null; }
field() { stats "$1" | awk -v k="$2" '$1 == k {print $3; exit}'; }
rep_field() {
    stats "$1" | awk -v k="$2" '
      $1 == "replica" {
        for (i = 1; i <= NF; i++) if ($i == k) {
          v = $(i + 1); gsub(/[^0-9]/, "", v); print v; exit
        }
      }'
}
fence_value() { [ -f "$1/fence" ] && cat "$1/fence" || echo 0; }
port_open() { (exec 3<>"/dev/tcp/${1%%:*}/${1##*:}") 2>/dev/null; }

start_server() { # name dir port log
    local name=$1 dir=$2 port=$3 log=$4
    "$R/treedb-server" --dir "$dir" --listen "$port" --workers 2 \
        >"$log" 2>&1 &
    PIDS+=("$!")
    wait_stats "$port" "$log"
}
start_standby() { # name primary dir port log
    local name=$1 primary=$2 dir=$3 port=$4 log=$5
    "$R/treedb-standby" --primary "$primary" --dir "$dir" --listen "$port" \
        --workers 2 --interval-ms 2 >"$log" 2>&1 &
    PIDS+=("$!")
    wait_stats "$port" "$log"
}
wait_stats() { # port log
    local port=$1 log=$2 i
    for i in $(seq 1 400); do
        stats "$port" >/dev/null 2>&1 && return 0
        sleep 0.05
    done
    tail -20 "$log" >&2
    fail "$port 没有进入可查询状态"
}
stop_pid() { # pid log
    local pid=$1 log=$2 i rc state
    kill -TERM "$pid" 2>/dev/null || return 1
    for i in $(seq 1 200); do
        kill -0 "$pid" 2>/dev/null || break
        state=$(ps -o stat= -p "$pid" 2>/dev/null | cut -c1)
        [ "$state" = Z ] && break
        sleep 0.05
    done
    if kill -0 "$pid" 2>/dev/null && [ "$(ps -o stat= -p "$pid" 2>/dev/null | cut -c1)" != Z ]; then
        kill -KILL "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
        forget_pid "$pid"
        tail -20 "$log" >&2
        fail "pid $pid 未在 10 s 内优雅退出"
        return 1
    fi
    wait "$pid"; rc=$?
    forget_pid "$pid"
    if [ "$rc" -ne 0 ]; then tail -20 "$log" >&2; fail "pid $pid 退出码 $rc"; return 1; fi
}
wait_replica() { # replica primary replica_log
    local rp=$1 pp=$2 log=$3 i rn pn rs rpseq lag
    for i in $(seq 1 400); do
        rn=$(field "$rp" nodes); pn=$(field "$pp" nodes)
        rs=$(field "$rp" seq); rpseq=$(field "$pp" seq)
        lag=$(rep_field "$rp" lag)
        if [ -n "$rn" ] && [ -n "$pn" ] && [ -n "$rs" ] && [ -n "$rpseq" ] && \
           [ "${lag:-x}" = 0 ] && [ "$rn" = "$pn" ] && [ "$rs" = "$rpseq" ]; then
            return 0
        fi
        sleep 0.05
    done
    stats "$rp" >&2 || true
    tail -20 "$log" >&2
    fail "副本未追平：replica nodes=$rn seq=$rs lag=${lag:-缺失}; primary nodes=$pn seq=$rpseq"
}
bench_create() { # port outfile
    TREEDB_BENCH_WARM=100 timeout 25 "$R/treedb-bench" --addr "$1" --op create \
        --conns 2 --threads 2 --views 4 --secs "$SECS" >"$2" 2>&1
}
bench_ok() { # rc outfile label
    local rc=$1 out=$2 label=$3 line
    [ "$rc" -eq 0 ] || { cat "$out" >&2; fail "$label bench 退出码 $rc"; return 1; }
    line=$(tail -1 "$out")
    printf '  %s\n' "$line"
    echo "$line" | grep -q 'errors=0' || { fail "$label bench 有错误"; return 1; }
    echo "$line" | grep -Eq '→ [1-9][0-9]* ops/s' || { fail "$label bench 没有正吞吐"; return 1; }
}

main() {
    local a_nodes a_seq b_nodes b_seq b_nodes0 b_lag b_errors c_nodes c_seq
    local a0 a1 b0 a_pid b_pid locked_rc ro_rc rc

    printf '===== %s HEAD %s protocol %s =====\n' "$(date '+%F %T')" \
        "$(git rev-parse --short HEAD)" "$(grep -m1 '^pub const PROTOCOL_VERSION' src/engine/mod.rs | grep -oE '[0-9]+' | tail -1)"
    echo "work=$D ports=$P1,$P2,$P3,$P4 secs=$SECS"

    say "1. 起真主 A，写入"
    start_server A "$D/A" "$P1" "$D/a.log" || return 1
    a_pid=${PIDS[0]}
    bench_create "$P1" "$D/bench-a1.log"; rc=$?
    bench_ok "$rc" "$D/bench-a1.log" A || return 1
    stats "$P1" | sed 's/^/A: /'
    a_nodes=$(field "$P1" nodes); a_seq=$(field "$P1" seq)
    assert_gt "$a_nodes" 0 "A.nodes" || return 1

    say "2. 起真副本 B，等 nodes/seq 追平且 lag=0"
    start_standby B "$P1" "$D/B" "$P2" "$D/b.log" || return 1
    b_pid=${PIDS[${#PIDS[@]}-1]}
    wait_replica "$P2" "$P1" "$D/b.log" || return 1
    stats "$P2" | sed 's/^/B: /'
    b_nodes=$(field "$P2" nodes); b_seq=$(field "$P2" seq)
    assert_eq "$b_nodes" "$a_nodes" "B.nodes == A.nodes" || return 1
    assert_eq "$b_seq" "$a_seq" "B.seq == A.seq" || return 1
    assert_eq "$(fence_value "$D/B")" 0 "B.fence" || return 1
    grep -q 'fence=0' "$D/b.log" || { fail "B banner 未报 fence=0"; return 1; }

    say "3. 提升前 B 必须只读；B 活着时 --promote 必须被目录锁拒绝"
    b_nodes=$(field "$P2" nodes); b_seq=$(field "$P2" seq)
    TREEDB_BENCH_WARM=100 timeout 10 "$R/treedb-bench" --addr "$P2" --op create \
        --conns 1 --threads 1 --views 1 --secs 1 >"$D/readonly.out" 2>&1
    ro_rc=$?
    printf '  readonly bench rc=%s: %s\n' "$ro_rc" "$(head -1 "$D/readonly.out")"
    [ "$ro_rc" -ne 0 ] || { fail "副本竟接受了写"; return 1; }
    grep -q 'code: 30' "$D/readonly.out" || { cat "$D/readonly.out" >&2; fail "拒写不是 EROFS(30)"; return 1; }
    assert_eq "$(field "$P2" nodes)" "$b_nodes" "只读拒写后 B.nodes" || return 1
    assert_eq "$(field "$P2" seq)" "$b_seq" "只读拒写后 B.seq" || return 1

    "$R/treedb-standby" --dir "$D/B" --listen "$P4" --promote >"$D/promote-locked.out" 2>&1
    locked_rc=$?
    printf '  locked promote rc=%s: %s\n' "$locked_rc" "$(head -1 "$D/promote-locked.out")"
    [ "$locked_rc" -eq 1 ] || { fail "被锁时提升应退出 1，实得 $locked_rc"; return 1; }
    grep -q 'already locked' "$D/promote-locked.out" || { fail "错误未点名目录锁"; return 1; }
    grep -q "pid $b_pid" "$D/promote-locked.out" || { fail "错误未带持锁 pid $b_pid"; return 1; }
    assert_eq "$(fence_value "$D/B")" 0 "被锁提升后 B.fence" || return 1

    say "4. 按切主顺序停旧主 A、停副本 B，再离线提升 B"
    stop_pid "$a_pid" "$D/a.log" || return 1
    stop_pid "$b_pid" "$D/b.log" || return 1
    "$R/treedb-standby" --dir "$D/B" --listen "$P4" --promote >"$D/promote.out" 2>&1
    rc=$?; cat "$D/promote.out" | sed 's/^/  /'
    [ "$rc" -eq 0 ] || { fail "停服后的提升失败"; return 1; }
    grep -q 'fence 0 -> 1' "$D/promote.out" || { fail "提升没有恰好抬一格"; return 1; }
    assert_eq "$(fence_value "$D/B")" 1 "提升后 B.fence" || return 1
    if port_open "$P4"; then fail "--promote 不应监听 $P4"; return 1; else echo "  OK: --promote 没有开端口"; fi

    say "5. 冷拷同令牌 C；用 B 起新主，数据不变且写可进"
    cp -a "$D/B" "$D/C" || { fail "冷拷 C 失败"; return 1; }
    start_server B-new "$D/B" "$P3" "$D/b2.log" || return 1
    assert_eq "$(field "$P3" nodes)" "$b_nodes" "新主初始 nodes" || return 1
    assert_eq "$(field "$P3" seq)" "$b_seq" "新主初始 seq" || return 1
    bench_create "$P3" "$D/bench-b.log"; rc=$?
    bench_ok "$rc" "$D/bench-b.log" "new primary B" || return 1
    b0=$(field "$P3" seq); b_nodes0=$(field "$P3" nodes)
    assert_gt "$b0" "$b_seq" "新主 B.seq" || return 1
    assert_eq "$(fence_value "$D/B")" 1 "新主 B.fence" || return 1

    say "6. 旧主 A 回魂仍可写；新主 B 与高令牌副本 C 都不能接收它"
    start_server A-zombie "$D/A" "$P1" "$D/a2.log" || return 1
    a0=$(field "$P1" seq)
    bench_create "$P1" "$D/bench-a2.log"; rc=$?
    bench_ok "$rc" "$D/bench-a2.log" "old primary A" || return 1
    a1=$(field "$P1" seq); assert_gt "$a1" "$a0" "旧主本地 A.seq" || return 1
    assert_eq "$(field "$P3" seq)" "$b0" "旧主写入后新主 B.seq" || return 1
    assert_eq "$(field "$P3" nodes)" "$b_nodes0" "旧主写入后新主 B.nodes" || return 1

    c_nodes=$b_nodes; c_seq=$b_seq
    start_standby C "$P1" "$D/C" "$P4" "$D/c.log" || return 1
    for _ in $(seq 1 400); do
        b_errors=$(rep_field "$P4" errors)
        [ "${b_errors:-0}" -gt 0 ] && break
        sleep 0.05
    done
    b_lag=$(rep_field "$P4" lag)
    assert_gt "$b_errors" 0 "C.replica_errors" || return 1
    assert_gt "$b_lag" 0 "C.replica_lag" || return 1
    assert_eq "$(field "$P4" seq)" "$c_seq" "拒旧主后 C.seq" || return 1
    assert_eq "$(field "$P4" nodes)" "$c_nodes" "拒旧主后 C.nodes" || return 1
    assert_eq "$(fence_value "$D/C")" 1 "拒旧主后 C.fence" || return 1
    grep -q 'is fenced out' "$D/c.log" || { tail -20 "$D/c.log" >&2; fail "C 日志未点名 fencing"; return 1; }
    grep -m1 'is fenced out' "$D/c.log" | sed 's/^/C: /'
    timeout 10 "$R/treedb-bench" --addr "$P4" --op ping --conns 1 --threads 1 --views 0 --secs 1 \
        >"$D/c-ping.log" 2>&1 || { cat "$D/c-ping.log" >&2; fail "C 拒旧主后不能答读"; return 1; }
    tail -1 "$D/c-ping.log" | sed 's/^/C read: /'

    say "7. 全部断言通过"
    echo "RESULT: PASS"
}

exec > >(tee -a "$OUT") 2>&1
main
rc=$?
[ "$rc" -eq 0 ] || echo "RESULT: FAIL"
exit "$rc"
