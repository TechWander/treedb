#!/bin/bash
# 跨代混跑探针：拿**上一个 tag 真编译出来的旧服务端**，用当前树的客户端打它，
# 量「协议版本不符」在两条路径上各自是什么形状。
#
# 为什么要有这个脚本，`tests/` 里那几百条用例不够吗：不够，而且原理上就不够。
# 一个二进制里客户端与服务端必然同代，`PROTOCOL_VERSION` 抬到几都测不出混代。
# §4.1 说「布局变了必须抬版本」，§4.4 说「客户端会核对 `Challenge.proto`」，这两句
# 合起来到底把谁拦住了、没拦住的那一路错成什么样，只有两代真二进制对跑才知道。
#
# 四臂，前三个是判据、最后一个定性：
#   1) banner     两个服务端启动行里的 `protocol=` 必须不同（代际自证）
#   2) cross+auth 新客户端打旧服务端、两边都带密钥 → 必须 rc≠0 且文案点名版本不符
#   3) same+auth  新客户端打新服务端、带密钥 → 必须 rc=0（证明 2) 不是坏 key 假红）
#   4) cross+noauth 两边都不开认证 → 钉住**当前实测形状**：不报错，但读回的是错值
#      （`protocol = 0`、`ops = 0`、`wal … 0 B on disk`）。这一条是 characterization：
#      哪天非认证路径也加了版本交换，它会红，那时要改的是 §4.4 的措辞而不是删掉它。
#
# 用法：
#   benches/xgen_probe.sh [基线 tag=v0.5.1]
# 前置：`cargo build --release --bin treedb-server` 已跑过（当前树当 new 档）。
# 结果追加到 benches/results/xgen_probe.log；临时目录在 .matrix/xgen（已 gitignore）。
set -u
cd "$(dirname "$0")/.." || exit 1
BASE=${1:-v0.5.1}
ROOT=$(pwd)
SRC=$ROOT/.matrix/xgen/$BASE
WORK=$ROOT/.matrix/xgen/run
OUT=benches/results/xgen_probe.log
P1=${XGEN_PORT:-17871}   # 旧服务端（开认证）
P2=$((P1 + 1))           # 旧服务端（不开认证）
P3=$((P1 + 2))           # 新服务端（开认证）
mkdir -p "$(dirname "$SRC")" "$WORK" benches/results
# 只清理本脚本自己起的服务端：按 --dir 前缀匹配，绝不误伤同机其它实例
mine() { pgrep -f "treedb-server --dir $WORK" 2>/dev/null; }
cleanup() { for p in $(mine); do kill "$p" 2>/dev/null; done; wait 2>/dev/null; }
trap cleanup EXIT
cleanup; sleep 0.5
rm -rf "$WORK"; mkdir -p "$WORK"
NEW_SRV=$ROOT/target/release/treedb-server
OLD_SRV=$SRC/target/release/treedb-server
[ -x "$NEW_SRV" ] || { echo "先 cargo build --release --bin treedb-server"; exit 2; }
if [ ! -x "$OLD_SRV" ]; then
    mkdir -p "$SRC"
    git archive "$BASE" | tar -x -C "$SRC" || { echo "git archive $BASE 失败"; exit 2; }
    (cd "$SRC" && cargo build --release --bin treedb-server >/dev/null 2>&1) || {
        echo "$BASE 的 treedb-server 编译失败"; exit 2; }
fi
KEY=$WORK/cluster.key
head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$KEY"
[ -s "$KEY" ] || { echo "造不出密钥"; exit 2; }

{
echo "===== $(date '+%F %T') 基线 $BASE → $(git describe --tags --always 2>/dev/null) ====="
fail=

# 1) banner：各自起一次只为读启动行（--listen :0 拿临时端口）
bsrv() { # $1=bin $2=dir；stdout 打印 protocol 值
    "$1" --dir "$2" --listen 127.0.0.1:0 > "$2.log" 2>&1 &
    local p=$!
    sleep 1
    kill "$p" 2>/dev/null
    wait "$p" 2>/dev/null
    head -1 "$2.log" | grep -oE 'protocol=[0-9]+' | cut -d= -f2
}
PN=$(bsrv "$NEW_SRV" "$WORK/ban-new")
PO=$(bsrv "$OLD_SRV" "$WORK/ban-old")
echo "banner  new protocol=$PN  old protocol=$PO"
if [ -n "$PN" ] && [ -n "$PO" ] && [ "$PN" != "$PO" ]; then
    echo "  OK: 两代服务端的 protocol= 不同（混代在字节层确实存在）"
else
    echo "  FAIL: banner 没能区分代际（$PN vs $PO）"; fail=1
fi

# stats 臂：$1=端口 $2=是否带密钥(yes/no)
probe() {
    local a=""
    [ "$2" = yes ] && a="--auth-key-file $KEY"
    timeout 12 "$NEW_SRV" --stats --listen "127.0.0.1:$1" $a
}
up() { # $1=bin $2=端口 $3=dir；$4 起为额外参数
    local b=$1 p=$2 d=$3
    shift 3
    "$b" --dir "$d" --listen "127.0.0.1:$p" "$@" > "$d.srv.log" 2>&1 &
    SRV_PID=$!
    for _ in $(seq 1 100); do
        (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null && return 0
        sleep 0.05
    done
    return 1
}

# 2) 跨代 + 两边开认证 → 必须拒
up "$OLD_SRV" "$P1" "$WORK/cross-auth" --auth-key-file "$KEY" || echo "  FAIL: 旧服务端没起来"
out=$(probe "$P1" yes 2>&1); rc=$?
echo "cross+auth   rc=$rc :: $(echo "$out" | head -1)"
if [ "$rc" -ne 0 ] && echo "$out" | grep -q "peer speaks protocol"; then
    echo "  OK: 认证路径把混代拦下并点名版本"
else
    echo "  FAIL: 期望 rc≠0 且文案含 peer speaks protocol"; fail=1
fi
kill $SRV_PID 2>/dev/null; wait $SRV_PID 2>/dev/null

# 3) 同代 + 开认证 → 必须通（上一臂的对照）
up "$NEW_SRV" "$P3" "$WORK/same-auth" --auth-key-file "$KEY" || echo "  FAIL: 新服务端没起来"
out=$(probe "$P3" yes 2>&1); rc=$?
echo "same+auth    rc=$rc :: $(echo "$out" | grep -m1 protocol | tr -s ' ')"
if [ "$rc" -eq 0 ] && echo "$out" | grep -qE "protocol +=+ *$PN"; then
    echo "  OK: 同代带密钥正常（说明 2) 的红不是坏 key 造成的）"
else
    echo "  FAIL: 同代对照没通过，2) 那条结论不成立"; fail=1
fi
kill $SRV_PID 2>/dev/null; wait $SRV_PID 2>/dev/null

# 4) 跨代 + 都不开认证 → characterization：钉住「不报错但读回错值」
up "$OLD_SRV" "$P2" "$WORK/cross-noauth" || echo "  FAIL: 旧服务端没起来"
out=$(probe "$P2" no 2>&1); rc=$?
echo "cross+noauth rc=$rc :: $(echo "$out" | grep -m1 protocol | tr -s ' ')"
if [ "$rc" -eq 0 ] && echo "$out" | grep -qE "protocol +=+ *0"; then
    echo "  OK: 非认证路径没有闸门（错代照样 rc=0，且 proto 字段读成 0）"
else
    echo "  DIFF: 非认证路径的形状变了 —— 要么加了闸门（好事，去改 §4.4），要么探测本身坏了"
    echo "$out" | sed 's/^/      /'
    fail=1
fi
kill $SRV_PID 2>/dev/null; wait $SRV_PID 2>/dev/null

[ -z "$fail" ] || echo "RESULT: FAIL"
echo
} 2>&1 | tee -a "$OUT"
