#!/bin/bash
# 万级视图下的连接/内存形状（P0-3 的 C2）：证明连接数**不随视图数增长**，并给出
# 每多一个视图要多付多少内存、create 延迟会不会劣化。
#
# 形状：一个 treedb-server + 一个 grovefs data-server + 一个
# `grovefs meta-server --meta-store treedb://…`，然后逐档把视图建到
# {100, 1000, 5000, 10000}。**每档建完视图后把本档新建的每一个视图都挂一遍**
# （每个挂载都建一个驻留 store，卸载不回收）——连接数的横坐标是「驻留 store」数，
# 不是「建了几个视图」。测量：
#   - 引擎侧 established socket 数（`ss`，按引擎端口）与全程峰值 ——这是「连接轴」本身；
#   - meta-server 的 socket fd 数、VmRSS、每个驻留 store 的边际内存；
#   - 挂载内 200 次 create+unlink 的均值，看写延迟会不会随视图数劣化。
#
# 依赖：`grovefs` 必须是 `--features treedb` 的版本（默认档会在门口拒绝这个 scheme），
# 且需要 fusermount（挂载档）。用法：
#   benches/many_views.sh [视图数…]        默认 "100 1000 5000 10000"
#   GROVEFS_BIN=... TREEDB_BIN=... 覆盖二进制路径（默认找同级 grovefs 的 release）
#   SWEEP=200                       每档只等距抽这么多个视图挂（默认 0 = 全部）
#   CONC=8                          末尾加一个并发臂：8 个视图同时挂、各 200 pairs
#   GROVEFS_TREEDB_CONNS=32         配合 CONC 看池大小的影响（默认 4）
#
# 注意（踩过的坑）：**不要**在循环里「建一个视图→挂一次→卸掉」。`view create` 结尾
# 会 `rt.evict()` 清掉整个引擎缓存，读数会恒定成「已经是共享的」——那是个假象。
# 先把视图全部建完，再逐个挂。
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
GROVEFS_BIN=${GROVEFS_BIN:-$ROOT/../grovefs/target/release/grovefs}
TREEDB_BIN=${TREEDB_BIN:-$ROOT/target/release/treedb-server}
LEVELS=${*:-"100 1000 5000 10000"}
D="$ROOT/.manyviews"
export LC_ALL=C LANG=C

# The doc comment of --meta-store mentions treedb in EVERY build (it is not
# feature-gated), so ask the binary instead: a default build refuses the URI at
# startup with this exact sentence.
if timeout 3 "$GROVEFS_BIN" meta-server --listen 127.0.0.1:1 --meta-store treedb://127.0.0.1:1 2>&1 \
     | grep -q 'not a --meta-store scheme this build serves'; then
    echo "$GROVEFS_BIN has no --features treedb; point GROVEFS_BIN at such a build"
    exit 2
fi

newport(){ python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
wait_port(){ local a=$1; for _ in $(seq 1 400); do (echo >/dev/tcp/${a%%:*}/${a##*:}) 2>/dev/null && return 0; sleep 0.05; done; return 1; }
PIDS=""
cleanup(){
    fusermount -u "$D/mnt" 2>/dev/null || true
    for p in $PIDS; do kill "$p" 2>/dev/null || true; done
    wait 2>/dev/null || true
}
trap cleanup EXIT

rm -rf "$D"; mkdir -p "$D/treedb" "$D/srv" "$D/data" "$D/home" "$D/op" "$D/mnt"
TD=127.0.0.1:$(newport); MA=127.0.0.1:$(newport); CTL=127.0.0.1:$(newport); DA=127.0.0.1:$(newport)
"$TREEDB_BIN" --dir "$D/treedb" --listen "$TD" > "$D/engine.log" 2>&1 & PIDS="$PIDS $!"
wait_port "$TD" || { echo "engine did not come up"; exit 2; }
"$GROVEFS_BIN" data-server --listen "$DA" --data-dir "$D/data" > "$D/data.log" 2>&1 & PIDS="$PIDS $!"
"$GROVEFS_BIN" meta-server --listen "$MA" --meta-dir "$D/srv" --control-listen "$CTL" \
    --meta-store "treedb://$TD" --engine-idle-secs 3600 > "$D/meta.log" 2>&1 & MP=$!
PIDS="$PIDS $MP"
wait_port "$MA" || { echo "meta-server did not come up"; tail -5 "$D/meta.log"; exit 2; }
sleep 1
KEY=$(timeout 20 "$GROVEFS_BIN" apikey --meta-dir "$D/srv" create --label root --for admin --owner root 2>&1 \
      | awk '/^  key: /{print $2}')
[ -n "$KEY" ] || { echo "no admin key"; exit 2; }

conns(){ ss -tnH state established "( sport = :${TD##*:} )" 2>/dev/null | wc -l; }
socks(){ ls -l "/proc/$MP/fd" 2>/dev/null | grep -c 'socket:\['; }
rss_kib(){ awk '/^VmRSS:/{print $2}' "/proc/$MP/status" 2>/dev/null; }
create_view(){ timeout 60 "$GROVEFS_BIN" --home "$D/op" view --control-server "$CTL" \
      --api-key "$KEY" create --view "$1" >/dev/null 2>&1; }
mnt_load(){ # 挂一个视图，跑 $1 次 create+unlink，打印每次操作的微秒数
    local n=$1 v=$2 t0 t1
    "$GROVEFS_BIN" --home "$D/home" mount --view "$v" --mountpoint "$D/mnt" \
        --meta-server "$MA" --data-servers "$DA" --foreground > "$D/mnt.log" 2>&1 & local fp=$!
    PIDS="$PIDS $fp"
    for _ in $(seq 1 200); do findmnt -rn --mountpoint "$D/mnt" >/dev/null 2>&1 && break; sleep 0.05; done
    findmnt -rn --mountpoint "$D/mnt" >/dev/null 2>&1 || { echo "  (mount never came up)"; return 1; }
    t0=$(date +%s%N)
    for i in $(seq 1 "$n"); do : > "$D/mnt/p$i" 2>/dev/null; rm -f "$D/mnt/p$i" 2>/dev/null; done
    t1=$(date +%s%N)
    echo $(( (t1 - t0) / 1000 / n ))
    fusermount -u "$D/mnt" 2>/dev/null || true
    kill "$fp" 2>/dev/null || true
}

# 连接轴的横坐标不是「建了几个视图」，而是「驻留了几个 store」：store 在第一次挂载
# 时建（`MetaStore::open_treedb`），卸载不回收（idle 窗口 3600 s）。所以每档把**本
# 档新建的视图逐个挂一遍**——改前每挂一个多 4 条连接，改后全进程 4 条。顺序
# 挂而不是并发，是为了让 fd 数和连接数都只反映「驻留」而不是「同时在跑」。
MOUNTED=0 PEAK=0 SWEEP=${SWEEP:-0}
mount_view(){ # 挂视图 $1、读一次目录、卸掉；顺带把引擎侧连接峰值记进 PEAK
    "$GROVEFS_BIN" --home "$D/home" mount --view "$1" --mountpoint "$D/mnt" \
        --meta-server "$MA" --data-servers "$DA" --foreground > "$D/mnt.log" 2>&1 & local fp=$!
    for _ in $(seq 1 300); do findmnt -rn --mountpoint "$D/mnt" >/dev/null 2>&1 && break; sleep 0.02; done
    if ! findmnt -rn --mountpoint "$D/mnt" >/dev/null 2>&1; then
        echo "  (mount $1 never came up: $(tail -1 "$D/mnt.log"))" >&2
        kill "$fp" 2>/dev/null || true
        return 1
    fi
    ls "$D/mnt" >/dev/null 2>&1
    MOUNTED=$((MOUNTED + 1))
    local c; c=$(conns)
    [ "$c" -gt "$PEAK" ] && PEAK=$c
    [ $((MOUNTED % 500)) = 0 ] && echo "    ... $MOUNTED views mounted, engine_conns=$c" >&2
    fusermount -u "$D/mnt" 2>/dev/null || true
    kill "$fp" 2>/dev/null || true
    return 0
}

RSS0=$(rss_kib); rss_prev=$RSS0
echo "levels: $LEVELS  (each level: create the new views, then mount every one of them)"
echo "engine $TD  meta-server $MA  (idle window 3600s: nothing is evicted during the run)"
echo "$(printf '%8s %8s %10s %7s %7s %9s %8s %9s %9s %10s' views mounted engine_conns peak tcp_socks rss_KiB KiB/store ms/create "mnt op us")"
total=0
for lvl in $LEVELS; do
    prev=$total
    t0=$(date +%s%N)
    while [ "$total" -lt "$lvl" ]; do
        total=$((total + 1))
        create_view "v$total"
    done
    t1=$(date +%s%N)
    # 每次 create 的均壁钟（含 grovefs 进程启动，常量项；跨档可比）
    per_ms=$(( (t1 - t0) / 1000000 / (total - prev) ))
    # 挂本档新建的每一个视图（每个都建一个 store）；SWEEP>0 时只等距抽这么多个
    step=1
    if [ "$SWEEP" -gt 0 ] && [ $((total - prev)) -gt "$SWEEP" ]; then step=$(( (total - prev) / SWEEP )); fi
    for i in $(seq $((prev + 1)) "$step" "$total"); do
        mount_view "v$i" || break
    done
    us=$(mnt_load 200 "v$total") || us="-"
    rss=$(rss_kib)
    store=$((total - prev)); [ "$store" -gt 0 ] || store=1
    echo "$(printf '%8s %8s %10s %7s %7s %9s %8s %9s %9s %10s' "$total" "$MOUNTED" "$(conns)" "$PEAK" \
        "$(socks)" "$rss" $(( (rss - rss_prev) / store )) "$per_ms" "$us")"
    rss_prev=$rss
done

# 并发臂（可选，`CONC=8`）：池是每进程 `GROVEFS_TREEDB_CONNS`（默认 4）条，跨视图
# 并发共用它们——这一臂就是给「池大小是不是瓶颈」定价：同一档视图数、同样 N 个并发
# 挂载，池小（默认）跑一次、池大（`GROVEFS_TREEDB_CONNS=32`）跑一次。
if [ "${CONC:-0}" -gt 1 ]; then
    echo
    echo "concurrency arm: $CONC views mounted at once, 200 create+unlink each, pool=${GROVEFS_TREEDB_CONNS:-4} conns"
    t0=$(date +%s%N)
    CPIDS=""
    for c in $(seq 1 "$CONC"); do
        (
            v="v$((total - c + 1))" md="$D/mnt_c$c"
            mkdir -p "$md"
            "$GROVEFS_BIN" --home "$D/home_c$c" mount --view "$v" --mountpoint "$md" \
                --meta-server "$MA" --data-servers "$DA" --foreground > "$md.log" 2>&1 & local_fp=$!
            for _ in $(seq 1 300); do findmnt -rn --mountpoint "$md" >/dev/null 2>&1 && break; sleep 0.02; done
            if findmnt -rn --mountpoint "$md" >/dev/null 2>&1; then
                for i in $(seq 1 200); do : > "$md/p$i" 2>/dev/null; rm -f "$md/p$i" 2>/dev/null; done
                fusermount -u "$md" 2>/dev/null || true
            else
                echo "  (concurrent mount of $v never came up)" >&2
            fi
            kill "$local_fp" 2>/dev/null || true
        ) &
        CPIDS="$CPIDS $!"
    done
    # 只等这一臂的子 shell：裸 `wait` 会把 meta-server / data-server / 引擎也等进去。
    for p in $CPIDS; do wait "$p" 2>/dev/null || true; done
    t1=$(date +%s%N)
    n=$((CONC * 200))
    printf '  %s mounts x 200 pairs: %8s ms wall, %5s us/pair aggregate, engine_conns=%s\n' \
        "$CONC" $(( (t1 - t0) / 1000000 )) $(( (t1 - t0) / 1000 / n )) "$(conns)"
fi

echo "engine --stats:"
"$TREEDB_BIN" --listen "$TD" --stats 2>/dev/null | sed -n '1,2p;10p'
echo "interpretation: engine_conns/peak must stay flat (one process-wide pool per address, so
                the curve is decoupled from the number of resident stores); KiB/store is the
                marginal RSS of one more resident store; mnt op us is the mounted latency."
