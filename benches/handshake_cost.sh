#!/bin/bash
# 接入认证要付多少（DESIGN.md §4.4）。分两笔量，因为它们是两回事：
#
#   1) **建连风暴**：每条连接多两次往返 + 两次 HMAC。这是真成本，每条连接付一次。
#   2) **稳态数据面**：握手完成之后每个 op 只多一次 `Gate::allows_data()` 判等。
#      预期是零差 —— 量出差来就说明分派写错了（握手 op 挤进了提交线程，或者
#      数据 op 每次都回去查了一遍闸门）。
#
# 为什么第一笔才是主项：连接池滚动重启 / 万级视图挂载时，客户端是**成批**重连的，
# 那时付的是 N 次握手，不是 N×ops 次判等。第二笔单独看会让人以为认证很便宜，
# 第一笔单独看会让人以为认证很贵，两笔都要摆出来。
#
# 用法：benches/handshake_cost.sh [连接数=2000]
# 前置：`cargo build --release` 已跑过。
set -u
cd "$(dirname "$0")/.." || exit 1
N=${1:-2000}
PORT=${PORT:-17861}
# 两臂之间等多久：让上一批连接的 TIME_WAIT 退掉。N 越大该越大，否则第二臂
# 会撞上 EADDRNOTAVAIL（本机临时端口段只有两万多个可用）。
SETTLE=${SETTLE:-2}
ROOT=$(pwd)
SRV=$ROOT/target/release/treedb-server
BENCH=$ROOT/target/release/treedb-bench
[ -x "$SRV" ] && [ -x "$BENCH" ] || { echo "先跑 cargo build --release"; exit 1; }
D=$(mktemp -d /tmp/treedb-hs.XXXXXX)
SPID=""
trap 'kill $SPID 2>/dev/null; rm -rf "$D"' EXIT

# 32 原始字节：`Key::load` 认的第一种形状，也是运维现实里最省事的一种。
python3 -c "import os;open('$D/k.raw','wb').write(os.urandom(32))" || exit 1

rss_kib() { awk '/VmRSS/{print $2}' /proc/"$1"/status; }

arm() { # $1=标签，其余=两边共用的 --auth-key-file 参数（空 = 不认证）
    local tag=$1; shift
    local auth=("$@")
    local d="$D/$tag"; mkdir -p "$d"

    "$SRV" --dir "$d" --listen "127.0.0.1:$PORT" --durability group \
        --auto-checkpoint-mb 0 "${auth[@]}" >"$d/srv.log" 2>&1 &
    SPID=$!
    local i=0
    until (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null; do
        i=$((i+1)); [ $i -gt 200 ] && { echo "$tag 服务端起不来：$(cat "$d/srv.log")"; exit 1; }
        sleep 0.05
    done

    # ---- 第 1 笔：建连风暴。时间用 bench 自报的那一行 ----
    # 脚本自己掐表会把「起进程 + 探活」也算进去（两端各几毫秒，N=2000 时
    # 能占两成的读数），而 bench 在连接循环前后各有一个 Instant，它报的才是
    # 「N 次 connect（+ 可选的两次往返握手）」本身。RSS 则在「建完、还没开始
    # 逐条 ping」的那一刻读：那时服务端表里正好挂着 N 条连接。
    local r0 conn_ms opened r1 held_ms t0 t1
    r0=$(rss_kib $SPID)
    t0=$(date +%s%N)
    "$BENCH" --addr "127.0.0.1:$PORT" --hold --conns "$N" --secs 30 \
        "${auth[@]}" >"$d/hold.log" 2>&1 &
    local bp=$!
    until grep -q '^connected ' "$d/hold.log" 2>/dev/null; do
        kill -0 $bp 2>/dev/null || { echo "$tag 建连失败：$(cat "$d/hold.log")"; exit 1; }
        sleep 0.02
    done
    r1=$(rss_kib $SPID)
    opened=$(sed -nE 's/^connected ([0-9]+) conns in ([0-9]+) ms$/\1/p' "$d/hold.log")
    conn_ms=$(sed -nE 's/^connected ([0-9]+) conns in ([0-9]+) ms$/\2/p' "$d/hold.log")
    # 等存活确认那一行也打完再收尾（那一轮 ping 是数据面，不计入 conn_ms）。
    until grep -q '^held ' "$d/hold.log" 2>/dev/null; do
        kill -0 $bp 2>/dev/null || { echo "$tag 没打完存活确认：$(cat "$d/hold.log")"; exit 1; }
        sleep 0.02
    done
    t1=$(date +%s%N)
    held_ms=$(( (t1 - t0) / 1000000 ))
    [ "$opened" = "$N" ] || echo "  ⚠ $tag 只建上 $opened/$N 条（端口段或 fd 不够），本臂数字不可用"
    kill $bp 2>/dev/null; wait $bp 2>/dev/null
    # 等对端 FIN 走完再量稳态，否则上一批连接还在服务端的 epoll 表里。
    sleep 0.5

    # ---- 第 2 笔：稳态。纯读一问一答 + 纯写（group commit 的合并度） ----
    # 两笔都是在闸门已经拉开之后测的：数据面每 op 只多一次 `allows_data()` 判等。
    local ping write
    ping=$("$BENCH" --addr "127.0.0.1:$PORT" --op ping --conns 64 --threads 64 \
                --pipeline 1 --secs 4 "${auth[@]}" | tail -1 | parse)
    write=$("$BENCH" --addr "127.0.0.1:$PORT" --op create --conns 32 --threads 32 \
                 --pipeline 1 --views 32 --secs 4 "${auth[@]}" | tail -1 | parse)

    # 字段用 TAB 分：bench 那行本身就带「|」，不能再拿它当分隔符。
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$tag" "$r0" "$r1" "$conn_ms" "$opened" "$held_ms" "$ping" "$write" \
        > "$D/$tag.row"
    kill $SPID 2>/dev/null; wait $SPID 2>/dev/null; SPID=""
    # 给上一批连接留出退 TIME_WAIT 的时间：本地端口段不够复用时，下一臂会直接
    # connect 不上（EADDRNOTAVAIL），那是一种假失败。
    sleep "$SETTLE"
}

parse() { # 把一行原始 bench 输出压成 "op ops/s p50 p99"
    sed -E 's/^op=([a-z]+).* → ([0-9]+) ops\/s.*p50=([0-9]+).*p99=([0-9]+).*/\1 \2 ops\/s p50=\3us p99=\4us/'
}

# rev 带 dirty 标记：本轮改动尚未提交时，光一个 HEAD 哈希会把「工作树」误标成
# 「已发布那一版」，而这两者的数据面代码不一样。
REV=$(git rev-parse --short HEAD)$(git diff --quiet 2>/dev/null || echo -dirty)
echo "=== 握手成本：$N 条连接（loopback，release，$(date -u '+%F %T UTC')，rev=$REV） ==="
arm off
arm on --auth-key-file "$D/k.raw"

for tag in off on; do
    IFS=$'\t' read -r _ r0 r1 conn opened held ping write < "$D/$tag.row"
    d=$(( r1 - r0 ))         # KiB
    per=$(( d * 1024 / N ))  # B/conn
    printf '\n[%s]\n' "$tag"
    printf '  建连 %s 条（实测 %s 条）: %s ms（%s ms/千条）\n' "$N" "$opened" "$conn" \
        "$(awk -v m="$conn" -v n="$N" 'BEGIN{printf "%.2f", m*1000/n}')"
    printf '  建完到存活确认       : %s ms（多出来的是 N 次 ping，与认证无关）\n' "$held"
    printf '  服务端 RSS           : %s KiB → %s KiB（+%s KiB，每连接 %s B）\n' \
        "$r0" "$r1" "$d" "$per"
    printf '  稳态 ping            : %s\n' "$ping"
    printf '  稳态 create（写）    : %s\n' "$write"
done

echo
echo '读法：两行的「稳态」应当同量级（握手不进引擎、不占提交线程）；'
echo '     「建连」那一列的差才是握手的代价（每条连接多两次往返 + 两次 HMAC）。'
echo '     RSS 那一列的差是每连接 `Conn.auth` 这个定长字段占的位置 —— 它是'
echo '     固定布局，握手完成后也不会还回来，所以只能靠连接数放大才量得出。'
