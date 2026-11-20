#!/bin/bash
# 三臂同口径对比：treedb（dur=group）/ PostgreSQL 12 / SQLite3。
#
# 口径（三臂逐项对齐，任何一条不齐这张表就不可比）：
#   1. 数据集：每个连接一条自己的命名空间，里面 500 条预热文件。
#      treedb 侧 `--views 1` + bench 自己按线程建 `bench{t}v0`；
#      PG/SQLite 侧 `--views $conns` + 线程 t 只碰第 t 个 schema/表。
#      （PG 的 `t % tables.len()` 在 views=conns 时正好把每条线程钉在自己那份上。）
#   2. 客户端线程数 = 连接数（每线程一条连接），三臂一致。
#   3. 计时窗口 = 每线程连上并设完参数之后起算，汇总取各线程窗口的最大值当分母；
#      建连与预热不进分母（DESIGN §7.5 的那个教训）。
#   4. ops 计数 = 真正执行完的语句/请求数，不按 pipeline 乘。
#   5. 落盘强度：treedb `--durability group`；PG `fsync=on` + `synchronous_commit=on`
#      （脚本读回闸门，不是 on 就 exit 1）；SQLite `journal_mode=WAL` + `synchronous=FULL`
#      （每条连接都设，且读回 journal_mode）。
#   6. pipeline=32 在 PG/SQLite 侧是「一个事务 32 条语句」（一次 commit 摊给 32 个 op），
#      在 treedb 侧是「一次往返 32 个请求」（服务端本轮一次 fsync 摊给 32 个 op）。
#      两边摊的是同一笔账（每 op 的 fsync 次数），但机制不同：报告里必须写明。
#   7. 每格都从空状态起跑：treedb 删数据目录重建；PG 把 `grovefs_v_*` schema
#      全 drop、`grovefs_refs.holders` 清空（否则 readdir 的 ORDER BY 要扫过
#      上一格累积下来的几千行，量出来的是「PG 越跑越慢」而不是「PG 慢」）；
#      SQLite 删库文件（含 -wal/-shm）。checkpoint 阈值都关掉/置为等价。
#
# 用法：benches/three_way.sh            # 默认 3 秒/格
#       SECS=5 benches/three_way.sh     # 跑久一点
#       OPS="lookup create" CONNS="1 64" benches/three_way.sh
set -u

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIN="$ROOT/target/release"
DIR="$ROOT/.spot/three"
PGDATA="$ROOT/.pgref"
PGPORT=${PGPORT:-54391}
PORT=${PORT:-7795}
SECS=${SECS:-3}
WORKERS=${WORKERS:-8}
OPS=${OPS:-"lookup readdir create write"}
CONNS=${CONNS:-"1 8 64"}
PIPES=${PIPES:-"1 32"}
RES="$ROOT/benches/results"
OUT=${OUT:-$RES/three_way.tsv}
RAW=${RAW:-$RES/three_way_raw.log}
SQLDB="$ROOT/.spot/three.db"
URL="postgresql://postgres@127.0.0.1:$PGPORT/postgres"

mkdir -p "$ROOT/.spot" "$RES"
: > "$OUT"
: > "$RAW"

psqlq() { psql -h 127.0.0.1 -p "$PGPORT" -U postgres -Atq -c "$1"; }

die() { echo "FATAL: $*" >&2; exit 1; }

pg_gate() {
    local f s v
    f=$(psqlq "show fsync") || die "psql 连不上 127.0.0.1:$PGPORT"
    s=$(psqlq "show synchronous_commit")
    [[ "$f" == "on" && "$s" == "on" ]] || die "PG 落盘强度不对：fsync=$f synchronous_commit=$s"
    v=$(psqlq "show server_version")
    echo "PG $v fsync=$f synchronous_commit=$s shared_buffers=$(psqlq 'show shared_buffers') max_wal_size=$(psqlq 'show max_wal_size')"
}

# 每格把 PG 清回空库（只清压测用的那几样，不碰集群里别的东西）。
pg_reset() {
    psqlq "DO \$do\$ DECLARE r record; BEGIN
             FOR r IN SELECT nspname FROM pg_namespace WHERE nspname LIKE 'grovefs\_v\_%' LOOP
               EXECUTE 'DROP SCHEMA ' || quote_ident(r.nspname) || ' CASCADE';
             END LOOP;
           END \$do\$;" >/dev/null || die "pg reset schemas"
    psqlq "DO \$do\$ BEGIN
             IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='grovefs_refs') THEN
               TRUNCATE grovefs_refs.holders;
             END IF;
           END \$do\$;" >/dev/null || die "pg reset refs"
}

wait_port() {
    local i
    for i in $(seq 1 400); do
        if (exec 3<>/dev/tcp/127.0.0.1/"$1") 2>/dev/null; then
            exec 3<&- 3>&- 2>/dev/null
            return 0
        fi
        sleep 0.05
    done
    return 1
}

emit() { # arm op conns pipe  <结果行>
    local arm=$1 op=$2 c=$3 p=$4 line=$5
    local q p50 p99 errs
    q=$(sed -n 's/.*→ \([0-9]*\) ops\/s.*/\1/p' <<<"$line")
    p50=$(sed -n 's/.*p50=\([0-9]*\).*/\1/p' <<<"$line")
    p99=$(sed -n 's/.*p99=\([0-9]*\).*/\1/p' <<<"$line")
    errs=$(sed -n 's/.*errors=\([0-9]*\).*/\1/p' <<<"$line")
    [[ -n "$q" ]] || { echo "PARSE FAIL: $line" >&2; q=0; p50=0; p99=0; errs=0; }
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$arm" "$op" "$c" "$p" "$q" "$p50" "$p99" >>"$OUT"
    printf '%s\t%s\tc=%-2s p=%-2s %8s ops/s p50=%-7s p99=%-7s errs=%s\n' \
        "$arm" "$op" "$c" "$p" "$q" "$p50" "$p99" "$errs" | tee -a "$RAW"
}

# treedb 一格：新建数据目录 → 起服务 → 跑 → 同格取 stats（写档证明 syncs==writes）→ 优雅停服。
tcell() { # op conns pipe workers
    local op=$1 c=$2 p=$3 w=$4
    rm -rf "$DIR"
    mkdir -p "$DIR"
    "$BIN/treedb-server" --dir "$DIR" --listen "127.0.0.1:$PORT" --durability group \
        --auto-checkpoint-mb 0 --workers "$w" >"$DIR/srv.log" 2>&1 &
    local pid=$!
    wait_port "$PORT" || { echo "treedb server 起不来（c=$c p=$p w=$w）" >&2; cat "$DIR/srv.log" >&2; kill "$pid" 2>/dev/null; return 1; }
    local line st
    line=$("$BIN/treedb-bench" --addr "127.0.0.1:$PORT" --op "$op" --conns "$c" --threads "$c" \
        --pipeline "$p" --secs "$SECS" --views 1 2>"$DIR/bench.err" | tail -1)
    st=$("$BIN/treedb-server" --listen "127.0.0.1:$PORT" --stats 2>/dev/null | tr '\n' '|')
    "$BIN/treedb-server" --listen "127.0.0.1:$PORT" --shutdown >/dev/null 2>&1
    wait "$pid" 2>/dev/null
    echo "  [stats w=$w] $st" >>"$RAW"
    sed -n 's/.*→ \([0-9]*\) ops\/s.*/  [raw] &/p' <<<"$line" >>"$RAW" 2>/dev/null
    emit "treedb-w$w" "$op" "$c" "$p" "$line"
}

pgcell() { # op conns pipe
    local op=$1 c=$2 p=$3
    pg_reset
    local line
    line=$("$BIN/treedb-pgbench" --uri "$URL" --op "$op" --conns "$c" --pipeline "$p" \
        --secs "$SECS" --views "$c" 2>>"$RAW" | tail -1)
    emit pg "$op" "$c" "$p" "$line"
}

scell() { # op conns pipe
    local op=$1 c=$2 p=$3
    rm -f "$SQLDB" "$SQLDB-wal" "$SQLDB-shm"
    local line
    line=$("$BIN/treedb-sqlitebench" --path "$SQLDB" --op "$op" --conns "$c" --pipeline "$p" \
        --secs "$SECS" --views "$c" 2>>"$RAW" | tail -1)
    emit sqlite "$op" "$c" "$p" "$line"
}

{
    echo "# 采集时间 $(date -Is)"
    echo "# $(uname -srm) / $(nproc) 核"
    echo "# treedb $(grep -m1 '^version' "$ROOT/Cargo.toml" | cut -d'"' -f2) workers=$WORKERS dir=$DIR"
    echo "# $(pg_gate)"
    echo "# sqlite $(sqlite3 --version)"
    echo "# disk: $(df -T "$ROOT" | tail -1)"
    echo "# $(lsblk -dno NAME,ROTA,MODEL 2>/dev/null | tr '\n' ';' | cut -c1-200)"
} | tee -a "$OUT"

for op in $OPS; do
    for c in $CONNS; do
        for p in $PIPES; do
            tcell "$op" "$c" "$p" "$WORKERS" || exit 1
            pgcell "$op" "$c" "$p"
            scell "$op" "$c" "$p"
        done
    done
done

# 单连接再补一档 workers=1：DESIGN §7.11 的历史基线就是这个档，读者要能对上账。
if [[ " $CONNS " == *" 1 "* ]]; then
    for op in $OPS; do
        for p in $PIPES; do
            tcell "$op" 1 "$p" 1 || exit 1
        done
    done
fi

rm -f "$SQLDB" "$SQLDB-wal" "$SQLDB-shm"
echo "# 完成：$OUT"
