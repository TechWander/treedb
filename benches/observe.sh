#!/bin/bash
# 接入面的两个运维旋钮（DESIGN.md §7.14）：`--max-conn` 与
# `--stats --format prometheus`。四档：
#   1) 上限：空载能进（正对照）→ 装满 → 超额全拒（含 64 条并发）→ 腾一格即恢复；
#      顺带证明「拒绝风暴没冲掉在册连接」与「拒绝日志限速」；
#   2) prometheus 档：结构三件套自检（每条指标 HELP/TYPE/样本各一行、名字唯一、
#      值都是数字），并与同实例的文本档逐项对账；
#   3) 三个失败形状：抓取不到 → 退出码 1 + stdout 恰好一行 `treedb_up 0`；
#      参数拼错 → 退出码 2 且不产出任何字节；
#   4) 停服汇总里的 `accepted / refused / peak` —— 一次连接的命运要能算清账。
#
# 用法：bash benches/observe.sh    → 输出追加到 benches/results/observe.log
# 全程只用临时目录与 127.0.0.1:17841，不碰真数据。
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT" || exit 1
BIN=./target/release
R=.spot/obs
OUT=benches/results/observe.log
PORT=17841
CAP=8
# 常驻连接与并发探测都是**子 shell**：fd 留在子进程里，父 shell 只拿 pid。
# 早期版本在父 shell 里直接 `exec 10<>...` 再 `<&10` 读，bash 对两位数 fd
# 的这条路会报 “错误的文件描述符”，而子 shell 的写法没有这个坑。
HOLDERS=""
SRVS=""
fail=0

# 等一个后台 pid 消失，最多 5 s。**不能裸 `wait`**：裸 wait 会把正在跑的
# 服务端也等一遍，脚本就挂在“收尾”这一步永远不退；而无上限地等某个 pid 又会
# 在 `--shutdown` 没送出去时把整条流水线拖死（第一次跑就是这样被 timeout 杀的）。
wait_gone() {
    local p=$1 i=0
    while kill -0 "$p" 2>/dev/null; do
        i=$((i + 1)); [ $i -gt 100 ] && return 1; sleep 0.05
    done
    wait "$p" 2>/dev/null
    return 0
}

stop_all() {
    for p in $HOLDERS $SRVS; do
        kill -TERM "$p" 2>/dev/null
        wait_gone "$p" || kill -KILL "$p" 2>/dev/null
    done
    HOLDERS=""
    SRVS=""
}
trap stop_all EXIT

mkdir -p .spot benches/results
rm -rf "$R"; mkdir -p "$R" "$R/storm"
say() { echo "$@" | tee -a "$OUT"; }

wait_up() {
    local i=0
    until (exec 3<>/dev/tcp/127.0.0.1/$1) 2>/dev/null; do
        i=$((i + 1)); [ $i -gt 200 ] && return 1; sleep 0.05
    done
}

start() { # DIR PORT ARGS...
    local dir=$1 port=$2; shift 2
    $BIN/treedb-server --dir "$dir" --listen 127.0.0.1:$port "$@" > "$R/srv.$port.log" 2>&1 &
    SRVS="$SRVS $!"
    wait_up "$port" || { say "!! $port 起不来：$(tail -2 "$R/srv.$port.log")"; return 1; }
    # 探活本身也占一个槽位；等它被回收，后面的计数才干净。
    sleep 0.1
}

# 探测一条**新**连接：0 = 连上并保持（还有空位），1 = 被拒（满了）。
# 判据是「读端等多久」：服务端接受后立刻关闭 → 读立刻拿到 EOF/ECONNRESET
# （bash 的 read 返回 1）；若一直没答且连接还在，读超时返回 142。
# 这两个值都是本机实测确认过的（见 §7.14 正文）。
probe() {
    ( exec 9<>/dev/tcp/127.0.0.1/$PORT 2>/dev/null || exit 1
      read -t 0.3 _ <&9; rc=$?
      [ "$rc" -eq 142 ] && exit 0
      exit 1 )
}

# 占一条常驻连接，直到被杀。**最后一个动作必须是内建 read，不能用 `sleep`**：
# 子进程会继承那个 fd，`kill` 掉子 shell 之后连接仍然开着（fd 3 还在 sleep 手里），
# 服务端等不到 EOF，槽位永不释放。第一次跑出来的「释放 1 个槽位后探测 ✗」、
# 档 2 两次 --stats 被拒、档 4 的 --shutdown 被拒（一共 70 次拒绝而不是 66）
# 全出自这一个坑。read 是内建、没有子进程，杀掉子 shell 就是整条连接关掉。
hold() {
    ( exec 3<>/dev/tcp/127.0.0.1/$PORT 2>/dev/null || exit 1
      IFS= read -r -t 300 _ <&3 ) &
    HOLDERS="$HOLDERS $!"
}

judge() { # RC 说明
    if [ "$1" -eq 0 ]; then say "  $2 ✓"; else say "  $2 ✗"; fail=$((fail + 1)); fi
}

say ""
say "=============== $(date '+%F %T')  接入面：--max-conn + prometheus（rev $(git rev-parse --short HEAD 2>/dev/null || echo '?')）"

# ---------------------------------------------------------------- 档 1：上限
say ""
say "[1] --max-conn $CAP --workers 4：正对照 → 装满 → 超额全拒 → 腾一格恢复"
start "$R/data" $PORT --workers 4 --max-conn $CAP --auto-checkpoint-mb 0 || exit 1

probe; judge $? "空载探测：连上并保持（正对照：判据不是一味返回“拒”）"

for _ in $(seq $CAP); do hold; done
sleep 0.3
held=$(echo $HOLDERS | wc -w)
say "  占满：$held 条常驻连接（上限 $CAP）"
! probe; judge $? "装满后探测：被拒"

# 64 条并发：验的是「多条 worker 各自 accept 时的原子性」——
# 读-比-加 的写法在这个形状下会超拨，先加再判超不会。
storm=""
for i in $(seq 64); do
    ( if probe; then : > "$R/storm/$i.ok"; else : > "$R/storm/$i.ref"; fi ) &
    storm="$storm $!"
done
for p in $storm; do wait "$p"; done
nok=$(ls "$R/storm"/*.ok 2>/dev/null | wc -l)
nref=$(ls "$R/storm"/*.ref 2>/dev/null | wc -l)
say "  并发 64 条超额探测：被拒 $nref / 仍连上 $nok（期望 64 / 0）"
[ "$nref" -eq 64 ] && [ "$nok" -eq 0 ]; judge $? "并发档全部被拒"

# 上限还满 ⇒ 下一条探测必须仍被拒。这就是「风暴没冲掉在册连接」的证据。
! probe; judge $? "风暴之后探测：仍被拒（8 个槽位都还在册）"

n1=$(grep -c 'refused connection #1 ' "$R/srv.$PORT.log" 2>/dev/null || true)
n2=$(grep -c 'refused connection #2 ' "$R/srv.$PORT.log" 2>/dev/null || true)
say "  拒绝日志：#1 出现 ${n1:-0} 次、#2 出现 ${n2:-0} 次（限速：第 1 条 + 每 1000 条）"
[ "${n1:-0}" -ge 1 ] && [ "${n2:-0}" -eq 0 ]; judge $? "拒绝日志已限速"

first=$(echo $HOLDERS | awk '{print $1}')
kill -TERM "$first" 2>/dev/null; wait_gone "$first" > /dev/null
HOLDERS=$(echo $HOLDERS | sed 's/^[0-9]* *//')
sleep 0.3
probe; judge $? "释放 1 个槽位后探测：连上并保持（上限算的是“同时在册”）"

# 后面几档要在同一个实例上做统计，先把常驻连接都还回去。
stop_all_holders() { for p in $HOLDERS; do kill -TERM "$p" 2>/dev/null; wait_gone "$p" > /dev/null; done; HOLDERS=""; }
stop_all_holders
# 槽位是在服务端读到 EOF 那轮才回收的（epoll 唤醒，通常亚毫秒），给一拍再抓指标。
sleep 0.3

# ---------------------------------------------------------------- 档 2：指标
say ""
say "[2] --stats --format prometheus：结构三件套 + 与文本档对账"
$BIN/treedb-server --listen 127.0.0.1:$PORT --stats --format prometheus > "$R/s.prom" 2> "$R/s.err"
prc=$?
lines=$(wc -l < "$R/s.prom")
help_n=$(grep -c '^# HELP ' "$R/s.prom" || true)
type_n=$(grep -c '^# TYPE ' "$R/s.prom" || true)
# 结构校验：每段 HELP/TYPE 各一次、样本名唯一、样本值都是数字、没有第四种行。
read -r smp bad < <(awk '
    /^# HELP / { help++; next }
    /^# TYPE / { type++; next }
    /^#/       { bad++; next }
    NF == 2 && $2 + 0 == $2 { if ($1 in seen) bad++; seen[$1] = 1; s++; next }
    { bad++ }
    END { print s + 0, bad + 0 }
' "$R/s.prom")
up=$(sed -n 's/^treedb_up //p' "$R/s.prom")
say "  退出码 $prc；${lines} 行 = HELP ${help_n} + TYPE ${type_n} + 样本 ${smp}（期望 27 指标 / 81 行）；结构错误 ${bad}"
# 27 是刻意钉死的：拓不到服务端时这里只会有一条 `treedb_up 0`，而“段数等于
# 样本数”对一份这样的文件也成立——不钉总数的话，两个空文件对账也能变绿。
[ "$prc" -eq 0 ] && [ "$bad" -eq 0 ] && [ "$smp" -eq 27 ] && [ "$help_n" -eq "$smp" ] && [ "$type_n" -eq "$smp" ]
judge $? "三件套齐、27 个指标、行数与段数自洽"
[ "$up" = "1" ] && [ ! -s "$R/s.err" ]; judge $? "treedb_up = 1 且 stderr 为空"

$BIN/treedb-server --listen 127.0.0.1:$PORT --stats > "$R/s.txt"
match=0
for k in views nodes dentries seq; do
    tv=$(sed -n "s/^$k *= *\([0-9]*\).*/\1/p" "$R/s.txt")
    pv=$(sed -n "s/^treedb_$k //p" "$R/s.prom")
    # `-n` 是防空对空：两侧都拓不到时会得到空串，那也是一定假绿。
    if [ -n "$tv" ] && [ "$tv" = "$pv" ]; then match=$((match + 1)); else say "  !! $k 文本档 [$tv] ≠ 指标档 [$pv]"; fi
done
say "  文本档与指标档同源：${match}/4 项相等（views/nodes/dentries/seq）"
[ "$match" -eq 4 ]; judge $? "两档逐项对账一致"
say "  指标样例：$(grep '^treedb_wal_logical_bytes\|^treedb_fsyncs_total\|^treedb_protocol_version' "$R/s.prom" | tr '\n' ' ')"

# ---------------------------------------------------------------- 档 3：失败形状
say ""
say "[3] 失败形状"
$BIN/treedb-server --listen 127.0.0.1:$((PORT + 1)) --stats --format prometheus > "$R/d.prom" 2> "$R/d.err"
drc=$?
dline=$(tr '\n' '|' < "$R/d.prom")
say "  连不上：退出码 $drc（期望 1），stdout = [$dline]，stderr = $(head -1 "$R/d.err")"
[ "$drc" -eq 1 ] && [ "$(sed -n 's/^treedb_up //p' "$R/d.prom")" = "0" ]
judge $? "抓取不到时输出 treedb_up 0 并以 1 退出"
for a in "--stats --format nope" "--format prometheus" "--max-conn abc"; do
    $BIN/treedb-server --listen 127.0.0.1:$PORT $a > "$R/e.out" 2> "$R/e.err"
    erc=$?
    esz=$(wc -c < "$R/e.out")
    say "  参数「$a」：退出码 $erc（期望 2）、stdout ${esz} B（期望 0）"
    [ "$erc" -eq 2 ] && [ "$esz" -eq 0 ]; judge $? "参数错按 usage error 处理"
done

# ---------------------------------------------------------------- 档 4：总账
say ""
say "[4] 优雅停服：一次连接的命运要能在汇总里算清"
$BIN/treedb-server --listen 127.0.0.1:$PORT --shutdown > "$R/shut.out" 2>&1
say "  --shutdown：$(tr '\n' ' ' < "$R/shut.out")"
grep -q 'shutdown acknowledged' "$R/shut.out"; judge $? "--shutdown 送达并得到确认"
for p in $SRVS; do wait_gone "$p" || say "!! 服务端 $p 收到 --shutdown 后 5 s 内没退"; done
SRVS=""
acct=$(grep -o 'accepted [0-9]* refused [0-9]* peak [0-9]*' "$R/srv.$PORT.log" | tail -1)
say "  $acct"
# 同一个实例上的每一次连接都要能对上：
#   accepted = 探活 1 + 正对照 1 + 常驻 8 + 释放后 1 + 两次 --stats 2 + --shutdown 1 = 14
#   refused  = 装满后 1 + 并发 64 + 风暴后 1 = 66（后面腾开过一格，且常驻已全还）
#   peak     = 上限 8
acc=$(echo "$acct" | sed -n 's/.*accepted \([0-9]*\).*/\1/p')
refused=$(echo "$acct" | sed -n 's/.*refused \([0-9]*\).*/\1/p')
peak=$(echo "$acct" | sed -n 's/.*peak \([0-9]*\).*/\1/p')
say "  accepted=$acc（期望 14） refused=$refused（期望 66） peak=$peak（期望 8）"
[ "${acc:-0}" -eq 14 ] && [ "${refused:-0}" -eq 66 ] && [ "${peak:-0}" -eq 8 ]
judge $? "连接总账三项都对得上（accepted / refused / peak）"

say ""
say "判定：$([ "$fail" -eq 0 ] && echo '全档通过 ✓' || echo "有 ${fail} 处 ✗")"
say "--- 完"
