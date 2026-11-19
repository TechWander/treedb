#!/bin/bash
# 单核口径的逐 op A/B：treedb `--workers 1` 对 PG，两边都钉在同一个核上。
#
# 为什么需要这第三种口径。历史矩阵里「单核那一列输给 PG」是拿 w=1 的**一个核**
# 去对 PG 的 **64 个 backend 摊在 128 个核上**，那个比值把两件事混在了一起：每核
# 效率与并行度。要回答「是不是缺索引 / 是不是没压榨」，必须先把并行度钉死：
#
#   t1   treedb --workers 1，taskset 钉 CPU $CPU             ← 被测的那一个核
#   t8   treedb --workers 8（本机默认档），不钉               ← 参考上限
#   p1   PG 私有集群，postmaster 钉同一个 CPU $CPU           ← 每核效率对照
#        （backend 是 fork 出来的、继承亲和性；脚本实测核验 Cpus_allowed_list）
#   pf   PG 私有集群，不限亲和性                              ← 历史严格判据
#
# 每格另记**测量窗口内服务端吃掉的核数**（benches/cores.py 10 Hz 采样累计 ticks，
# 事后按 bench 自报的窗口秒数回切 —— 不能用「跑前跑后各读一次除以 --secs」，那样
# 分子会把预热流量也算进来）。不记这个数，「一个核打赢 64 个进程」和「一个核打赢
# 一个核」就会被混成同一句话。
#
# 持久化口径：读 op（ping/lookup/readdir）两边都不 fsync（PG 走只读事务），treedb
# 用 `never`；写 op（create/write）treedb 用默认 `group`，PG 用 `synchronous_commit=on`，
# WAL 与 PGDATA 同在 /data 这块机械盘上。PG 侧 fsync/synchronous_commit 读回不过关
# 就直接中止，绝不出表。
#
# 用法：
#   benches/sc_ops.sh [每格秒数=3] [重复次数=3] [op…]
# 结果 append 到 benches/results/sc_ops.log。
set -u
cd "$(dirname "$0")/.." || exit 1
ROOT=$(pwd)
SECS=${1:-3}
REPS=${2:-3}
# 每一次运行有自己的目录后缀与端口基址。上一轮就是栽在这里：脚本按 `--dir /tmp/sc`
# 前缀 pkill，于是并发跑的两个实例互删服务端 —— 症状是「banner 已打印、几十毫秒后
# 优雅退出、ops 0」（它接受到的唯一一条连接就是就绪探针）。
RUN=${RUN:-r$$}
PORT=${PORT:-17851}
PGPORT=${PGPORT:-54390}
CPU=${CPU:-$(python3 benches/cores.py idle)}
PGBIN=/usr/lib/postgresql/12/bin
PGDATA="$ROOT/.pgref"
OUT=benches/results/sc_ops.log
# 采样文件必须每格一份：共用一个 /tmp/sc-cores.txt 时，上一格没死透的采样器会与
# 本格的双写者撞在同一文件上（各自 O_TRUNC），中间吃出一个 NUL 洞 —— 症状是
# cores=0.00，看上去像 PG 根本不花 CPU。
SAMP=/tmp/sc-cores-$RUN
SRV=""
SAMPLER=""
PGPM=""
mkdir -p benches/results

say() { printf '%s\n' "$*"; }
emit() { say "$*"; printf '%s\n' "$*" >> "$OUT"; }   # 不能用 cell|tee：那会把 cell 丢进子 shell，$SRV 丢了收不掉服务

# 同一把 flock：并发跑两个本脚本必然互扰，宁可直接拒绝第二份。
LOCK=/tmp/sc_ops.lock
exec 9>"$LOCK" || exit 1
flock -n 9 || { say "!! 已有 sc_ops.sh 在跑（flock $LOCK），本次拒绝启动"; exit 1; }

stop_sampler() { [ -n "$SAMPLER" ] && { kill "$SAMPLER" 2>/dev/null; wait "$SAMPLER" 2>/dev/null; }; SAMPLER=""; }
drop_srv() {
    stop_sampler
    [ -n "$SRV" ] && { kill "$SRV" 2>/dev/null; wait "$SRV" 2>/dev/null; }
    SRV=""
}
drop_pg() {
    [ -n "$PGPM" ] && kill "$PGPM" 2>/dev/null
    "$PGBIN/pg_ctl" -D "$PGDATA" -m immediate -w stop >/dev/null 2>&1
    PGPM=""
}
trap 'drop_srv; drop_pg' EXIT

wait_port() { # $1=port
    local i=0
    until (exec 3<>/dev/tcp/127.0.0.1/$1) 2>/dev/null; do
        i=$((i + 1)); [ $i -gt 200 ] && return 1
        sleep 0.05
    done
}

# 起服务端的三道守卫。本轮就差点又栽一次：一条早前被终端回显吞掉的后台命令留下
# 孤儿服务端占着端口，于是每一格的新服务端都 bind 失败立刻退出，bench 全部打在
# 那个孤儿上 —— t8 那一臂的数字因此与 t1 一模一样，看上去像「读并行回归」。
# 光靠「端口能连上」判定就绪是不够的，那只能证明**有个**服务端在听。
require_port_free() {
    (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null && {
        emit "!! 端口 $PORT 已被占用，先收掉残留：$(ss -ltnp 2>/dev/null | grep ":$PORT " | head -1)"
        exit 1
    }
    return 0
}
require_alive() { # $1=pid $2=目录 $3=标签
    kill -0 "$1" 2>/dev/null || { emit "!! $3 服务端进程已退出：$(tail -2 "$2/srv.log")"; return 1; }
    grep -q 'treedb-server pid=' "$2/srv.log" || { emit "!! $3 banner 没出现：$(tail -2 "$2/srv.log")"; return 1; }
    grep -qi 'address already in use\|bind ' "$2/srv.log" && { emit "!! $3 bind 失败：$(tail -2 "$2/srv.log")"; return 1; }
    # 必须显式 return 0：上一版以 `grep … && { … }` 收尾，一切正常时 grep 不匹配
    # 返回 1，函数把这个 1 当返回值 —— 于是每一格都被误判成失败，而且因为
    # 走的是「没匹配上」这条路，emit 一句都不打，整段 treedb 静默产出 0 格。
    return 0
}
require_workers() { # $1=目录 $2=wanted —— banner 里的 workers 必须等于参数
    local got=$(grep -oE 'workers=[0-9]+' "$1/srv.log" | head -1)
    [ "$got" = "workers=$2" ] || { emit "!! banner 是 $got、要的是 workers=$2"; return 1; }
    return 0
}

# 直接拉 postgres 前台进程（后台化由 shell 做），这样 $! 就是 postmaster 本身：
# pg_ctl -w start 的坑是它把 postmaster 开在子 shell 里，返回的 pid 不是被测那个。
# 就绪判定用 psql 轮询（TCP 端口能连上只代表 postmaster 在听，还不能接活）。
psql_wait() {
    export PGHOST=127.0.0.1 PGPORT=$PGPORT PGUSER=postgres PGDATABASE=postgres
    local i=0
    until psql -Atc 'select 1' >/dev/null 2>&1; do
        i=$((i + 1)); [ $i -gt 60 ] && return 1
        sleep 0.25
    done
}

# bench 一行输出 → "ops span p50 p99"
cut_line() { sed -E 's/.*→ ([0-9]+) ops\/s over ([0-9.]+)s.*p50=([0-9]+).*p99=([0-9]+).*/\1 \2 \3 \4/'; }

# tcell 臂 dur op workers 第n次 —— treedb 一格
tcell() {
    local arm=$1 dur=$2 op=$3 w=$4 n=$5
    local d=/tmp/sc-$RUN-$arm-$op
    local samp=$SAMP-$arm-$op-$n
    drop_srv
    require_port_free || return 1
    rm -rf "$d"; mkdir -p "$d"
    if [ "$arm" = t1 ]; then
        taskset -c "$CPU" ./target/release/treedb-server --dir "$d" --listen 127.0.0.1:$PORT \
            --durability "$dur" --auto-checkpoint-mb 0 --workers "$w" >"$d/srv.log" 2>&1 &
    else
        ./target/release/treedb-server --dir "$d" --listen 127.0.0.1:$PORT \
            --durability "$dur" --auto-checkpoint-mb 0 --workers "$w" >"$d/srv.log" 2>&1 &
    fi
    SRV=$!
    wait_port $PORT || { emit "$arm $op #$n 服务端起不来：$(cat "$d/srv.log")"; return 1; }
    require_alive "$SRV" "$d" "$arm/$op" || return 1
    require_workers "$d" "$w" || return 1
    python3 benches/cores.py sample "$SRV" threads "$samp" &
    SAMPLER=$!
    ./target/release/treedb-bench --addr 127.0.0.1:$PORT --op "$op" --conns 64 --threads 64 \
        --pipeline 1 --secs "$SECS" 2>"$d/err" | tail -1 | cut_line >"$d/r"
    stop_sampler
    local cores=$(python3 benches/cores.py calc "$samp" $(awk '{print $2}' "$d/r"))
    rm -f "$samp"
    local aff=$(awk '/^Cpus_allowed_list/{print $2}' /proc/$SRV/status 2>/dev/null)
    read -r ops span p50 p99 <"$d/r"
    emit "cell arm=$arm op=$op w=$w rep=$n ops=$ops p50=$p50 p99=$p99 cores=$cores cpu=$aff load=$(cut -d' ' -f1 /proc/loadavg)"
    [ -s "$d/err" ] && tail -2 "$d/err" | sed 's|^|  ! |' | tee -a "$OUT"
}

# pcell 臂 op 第n次 —— PG 一格（同一个集群喂多格，setup/warmup 只在起集群后付一次）
pcell() {
    local arm=$1 op=$2 n=$3
    local d=/tmp/sc-$RUN-pg-$op
    local samp=$SAMP-$arm-$op-$n
    mkdir -p "$d"
    python3 benches/cores.py sample "$PGPM" tree "$samp" &
    SAMPLER=$!
    ./target/release/treedb-pgbench --uri "postgresql://postgres@127.0.0.1:$PGPORT/postgres" \
        --op "$op" --conns 64 --pipeline 1 --secs "$SECS" 2>"$d/err" | tail -1 | cut_line >"$d/r"
    stop_sampler
    local cores=$(python3 benches/cores.py calc "$samp" $(awk '{print $2}' "$d/r"))
    rm -f "$samp"
    read -r ops span p50 p99 <"$d/r"
    emit "cell arm=$arm op=$op rep=$n ops=$ops p50=$p50 p99=$p99 cores=$cores cpu=$(awk '/^Cpus_allowed_list/{print $2}' /proc/$PGPM/status 2>/dev/null) load=$(cut -d' ' -f1 /proc/loadavg)"
    [ -s "$d/err" ] && tail -2 "$d/err" | sed 's|^|  ! |' | tee -a "$OUT"
}

pg_start() { # $1=臂 p1|pf
    drop_pg
    # 绝不传 -F（那是关 fsync，不是 initdb 的 --no-sync）。
    local PGO="-p $PGPORT -c listen_addresses=127.0.0.1 -c unix_socket_directories=$PGDATA -c max_connections=4050 -c fsync=on -c synchronous_commit=on"
    if [ ! -f "$PGDATA/PG_VERSION" ]; then
        mkdir -p "$PGDATA"
        "$PGBIN/initdb" -D "$PGDATA" -U postgres --auth=trust -E UTF8 >/dev/null || exit 1
    fi
    # 直接把 postgres 拉后台，不用 pg_ctl -w start：一是 pg_ctl 不接 -l 时 postmaster
    # 继承调用方的 stderr，脚本被重定向到管道时整句死销（本轮就卡在这一句上）；
    # 二是 pg_ctl 自己退出后 postmaster 会被重新挂到 init 下，$! 不可信。
    local n=0 ok=0
    while [ $n -lt 3 ]; do
        n=$((n + 1))
        if [ "$1" = p1 ]; then
            taskset -c "$CPU" "$PGBIN/postgres" -D "$PGDATA" $PGO >"$PGDATA/log" 2>&1 &
        else
            "$PGBIN/postgres" -D "$PGDATA" $PGO >"$PGDATA/log" 2>&1 &
        fi
        PGPM=$!
        psql_wait && { ok=1; break; }
        emit "PG 启动第 $n 次没就绪（$1）：$(tail -3 "$PGDATA/log" | tr '\n' ' ')"
        drop_pg; sleep 1
        rm -f "$PGDATA/postmaster.pid"   # immediate stop 后的残留 pid 文件会让下一次直接退
    done
    [ "$ok" = 1 ] || { emit "PG 起不来（$1），见 $PGDATA/log"; exit 1; }
    sleep 0.3
    export PGHOST=127.0.0.1 PGPORT=$PGPORT PGUSER=postgres PGDATABASE=postgres
    local f=$(psql -Atc 'show fsync') s=$(psql -Atc 'show synchronous_commit')
    # 先开两条连接，再抽一个 backend 验亲和性真传下去了
    ./target/release/treedb-pgbench --uri "postgresql://postgres@127.0.0.1:$PGPORT/postgres" \
        --op ping --conns 2 --pipeline 1 --secs 1 >/dev/null 2>&1
    emit "pg 臂=$1 pid=$PGPM fsync=$f synchronous_commit=$s postmaster cpu=$(awk '/^Cpus_allowed_list/{print $2}' /proc/$PGPM/status) backends=$(pgrep -P "$PGPM" | wc -l)"
    if [ "$f" != on ] || [ "$s" != on ]; then emit "PG 持久化闸门不过（$1），作废"; exit 1; fi
}

# 第三项起才是 op 列表：不能直接用 $*，那会把前面两个数字参数也当 op 名。
OPS="${*:3}"
[ -n "$OPS" ] || OPS="ping lookup readdir create write"

emit "=== 单核口径逐 op A/B $(date -u '+%F %T UTC') CPU钉=$CPU SECS=$SECS REPS=$REPS ops=$OPS run=$RUN ==="
sleep 0.3   # 只等上一格的 socket 走完 TIME_WAIT；不 pkill（见上）
# 只收自己前缀的残留（崩溃留下的），不碰别人的目录 —— 这是 pkill 那版的修法。
pkill -f "treedb-server --dir /tmp/sc-$RUN-" 2>/dev/null || true
emit "机器：$(awk -F: '/model name/{print $2}' /proc/cpuinfo | head -1 | tr -d ' ') nproc=$(nproc) load=$(cut -d' ' -f1-3 /proc/loadavg)"
df -hT /data | tail -1 | awk '{print "盘：" $7 " " $6 " 挂载 " $2}' | tee -a "$OUT"

for n in $(seq 1 $REPS); do
    for op in ping lookup readdir; do tcell t1 never "$op" 1 "$n"; done
    for op in create write; do tcell t1 group "$op" 1 "$n"; done
    for op in ping lookup readdir; do tcell t8 never "$op" 8 "$n"; done
    for op in create write; do tcell t8 group "$op" 8 "$n"; done
done
drop_srv

# SKIP_PG=1 只跑 treedb 两臂：改一版代码想看同一格数字时不必再付一遍 PG 的
# 建集群/预热成本，而 PG 那两臂与 treedb 二进制无关，上一轮的基线照旧有效。
if [ "${SKIP_PG:-0}" != 1 ]; then
for n in $(seq 1 $REPS); do
    pg_start p1
    for op in $OPS; do pcell p1 "$op" "$n"; done
    drop_pg
    pg_start pf
    for op in $OPS; do pcell pf "$op" "$n"; done
    drop_pg
done
fi

emit "=== 汇总（REPS 次中位；cores = 测量窗口内服务端平均核数）==="
# 只汇总本次运行：$OUT 是 append 的，不按 run= 切一刀就会把上几个星期的
# 旧格混进中位数里 —— 那恰好就是「引用历史数字」，而这轮的任务就是不再干这事。
awk -v tag="run=$RUN" 'index($0,tag){f=1} f' "$OUT" | \
    python3 benches/sc_sum.py /dev/stdin "本次运行汇总（$RUN）" | tee -a "$OUT"
