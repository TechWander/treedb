#!/bin/bash
# 默认档（零旗标）复测：证明「照 README 抄两行命令起服务」就已经是并行档。
#
# 为什么要单独有这一条：§7.5 那两张表里的 treedb 数字都是矩阵显式传
# `--workers N` 跑出来的，而用户不会传 —— 他抄的是 README 那两行。默认值算成
# 几个前端、和显式给同样的值是否同格，必须单独钉一次。另外矩阵是 84 格连跑，
# 前一格 `rm -rf` 掉 25 万个文件留下的脏页回写会压到后一格头上（本轮就观察到
# readdir 那一格比独立复跑低 17–20 %），所以这里的数字**只能跟自己比**，
# 与矩阵同格的差就是环境项，§7.5 已按「保守取下界」处理。
#
# 用法：
#   benches/default_check.sh              # 默认 5 次重复、三个前端档位
#   REPS=3 PORT=17842 benches/default_check.sh
# 前置：`cargo build --release` 已跑过。
set -u
cd "$(dirname "$0")/.." || exit 1
REPS=${REPS:-5}
PORT=${PORT:-17841}
# 数据目录必须跟矩阵同盘（仓根 = /data 那块 161 TB 机械 XFS），否则 WAL 换到
# 根盘 ext4 上，这一条与 §7.5 的格子就不是同一件事了。
ROOT=$(pwd)
DDIR=$ROOT/.matrix/dc
mkdir -p "$ROOT/.matrix"
BENCH=target/release/treedb-bench
SRV=target/release/treedb-server
[ -x "$BENCH" ] && [ -x "$SRV" ] || { echo "先 cargo build --release"; exit 1; }

say() { printf '%s\n' "$*"; }
say "=== 默认档复测 $(date -u '+%F %T UTC')  核数=$(nproc)  REPS=$REPS ==="

arm() { # $1=标签 $2..=服务端旗标（空 = 一个都不给）
    local tag=$1; shift
    local op i pid d line
    for op in readdir lookup ping; do
        for i in $(seq 1 "$REPS"); do
            d=$DDIR-$op-$i
            rm -rf "$d"; mkdir -p "$d"
            "$SRV" --dir "$d" --listen 127.0.0.1:$PORT --auto-checkpoint-mb 0 \
                "$@" >"$d/srv.log" 2>&1 &
            pid=$!
            local n=0
            until (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null; do
                n=$((n+1)); [ $n -gt 100 ] && { say "$tag 起不来：$(cat "$d/srv.log")"; return 1; }
                sleep 0.05
            done
            [ "$i" = 1 ] && say "srv[$tag]: $(cat "$d/srv.log")"
            line=$($BENCH --addr 127.0.0.1:$PORT --op "$op" --conns 64 --threads 64 \
                    --pipeline 1 --secs 3 2>&1 | tail -1)
            printf '%-8s %-8s run%s → %s\n' "$tag" "$op" "$i" \
                "$(printf '%s' "$line" | sed -E 's/^op=([a-z]+).* → ([0-9]+ ops\/s over [0-9.]+s).*p50=([0-9]+).*max=([0-9]+).*/\2 | p50=\3 max=\4 us/')"
            kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
            rm -rf "$d"
        done
    done
}

arm "默认"
arm "w8显式" --workers 8
# 第三臂不是多余的：§7.5 里「单核那一列输 PG 0.26–0.48×」用的也是矩阵连跑
# 的格子，而本脚本已经证明连跑会把读档压掉 1.3–1.7× —— 那一列得单独重跑
# 一次才能报。它同时是「默认值与显式值同格」的反例：w=1 明显低于前两臂。
arm "w1单核" --workers 1
