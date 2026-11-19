#!/bin/bash
# 定位 readdir 在服务端到底吃在哪：exec 只占 9.8 µs（treedb-micro），但
# workers=1 / conns=64 那一格量到 53 µs/op。先分清「服务端 CPU 打满没有」
# 与「一次 op 付几次 syscall」，再决定改哪里。
#
# 用法：benches/probe_srv.sh [dur=never] [op=readdir] [conns=64] [workers=1]
# DIR=/data/grovefs/treedb/.probe ./benches/probe_srv.sh   # 把 WAL 换成慢盘验证 fsync 影响
set -u
cd /data/grovefs/treedb || exit 1
PORT=17811
DIR=${DIR:-/tmp/probe-rd}
DUR=${1:-never}
OP=${2:-readdir}
CONNS=${3:-64}
WORKERS=${4:-1}
rm -rf "$DIR"; mkdir -p "$DIR"
./target/release/treedb-server --dir "$DIR" --listen 127.0.0.1:$PORT \
    --durability "$DUR" --auto-checkpoint-mb 0 --workers "$WORKERS" >"$DIR/srv.log" 2>&1 &
SRV=$!
trap 'kill $SRV 2>/dev/null; wait $SRV 2>/dev/null' EXIT
i=0
until (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null; do
    i=$((i+1)); [ $i -gt 100 ] && { cat "$DIR/srv.log"; exit 1; }
    sleep 0.05
done

run_bench() { ./target/release/treedb-bench --addr 127.0.0.1:$PORT --op "$OP" \
    --conns "$CONNS" --threads "$CONNS" --pipeline 1 --secs "$1"; }

# 先跑一轮把库建好（bench 的预热阶段在计时窗外，两轮互不干扰）。
run_bench 3 >/dev/null 2>&1

clk=$(getconf CLK_TCK)
read_cpu() { awk -v clk="$clk" '{print ($14+$15)/clk}' /proc/$SRV/stat; }

# 阶段 1：不挂 ptrace，量干净的服务端 CPU 占比 —— 这才是「53 µs/op 是不是
# 全花在服务端这一个核上」的答案。
run_bench 8 >"$DIR/cpu.out" 2>&1 &
B1=$!
sleep 2
c0=$(read_cpu); t0=$(date +%s.%N)
sleep 4
c1=$(read_cpu); t1=$(date +%s.%N)
wait $B1 2>/dev/null

# 阶段 2：另起一轮，中途挂 3 秒 strace 看 syscall 配方。strace 会把被追
# 进程拖慢一个量级，故这一段只看比例（read/write/epoll_wait 是否 1:1:1）。
run_bench 6 >"$DIR/st.out" 2>&1 &
B2=$!
sleep 2
timeout 3 strace -c -o /tmp/probe.st -p "$SRV" >/dev/null 2>&1 || true
wait $B2 2>/dev/null
kill $SRV 2>/dev/null; wait $SRV 2>/dev/null; trap - EXIT

awk -v c0="$c0" -v c1="$c1" -v t0="$t0" -v t1="$t1" 'BEGIN{
  dt=t1-t0; cpu=c1-c0;
  printf "阶段 1：窗口 %.1fs 服务端 CPU %.2fs → %.0f%% 单核\n", dt, cpu, cpu/dt*100
}'
grep -E "ops/s" "$DIR/cpu.out" | head -1
echo "--- 阶段 2（挂 strace 的 3 秒内，吞吐仅供参照）---"
grep -E "ops/s" "$DIR/st.out" | head -1
tail -12 /tmp/probe.st
