#!/bin/bash
# 「按表分实例」到底能不能把写吞吐推上去 —— 回答「单进程是不是写瓶颈」。
#
# 两模式、同一负载形状对照：
#   split S   S 个 treedb-server 进程，各占一个 --dir、一个端口，每个进程配一个
#             bench 客户端（PER 条连接）。实例之间零共享：没有同一条 WAL、没有
#             同一个提交线程。这就是「按表 hash 路由到实例」那个部署方案的形状。
#   solo  S   1 个 treedb-server 进程，S 个 bench 客户端各打自己那张表（表名靠
#             TREEDB_BENCH_TABLE 分开）。客户端侧的进程数、连接数、pipeline 与
#             split 逐格相同 —— 唯一差别是服务端只有一路提交线程。
#
# 为什么对照组是「S 个客户端进程压 1 个服务端」，而不是「1 个客户端开 S*PER 条
# 连接」：后者会把客户端自己变成瓶颈（一个进程里几十条线程抢 CPU），那时 solo 的
# 数字里混着「测量工具跑不动」，分不出是服务端定序的墙还是工具的问题。两边都固定
# 「每实例 PER 条连接、每进程 PER 条线程」，唯一被研究的变量就是实例数。
#
# 每格还报服务端 CPU 占用（核数），这是判据本身：
#   - solo 的单个服务端顶到 ~1 核而吞吐不再涨 → 提交线程这一路到顶，分片才有意义；
#   - solo 的 CPU 远不到 1 核而吞吐也不涨 → 瓶颈在别处（盘、客户端、锁），
#     就不能拿「单进程是瓶颈」下结论；
#   - split 每实例 ~1 核、总量近线性 → 进程级分片确实能扩。
#
# 三条自检（不做断言就是假绿）：
#   1) 每个 bench 进程 `errors=0`：`rowinsert` 用 PUT_IF_ABSENT，键撞上回 EEXIST，
#      而那种格子照样能跑出漂亮的 ops/s（回包本身是成功的往返）；
#   2) 服务端 `rows` 必须与发出去的 op 数对得上（±2%）：这证明数字是「真插进去了」，
#      不是「表名不存在所以早退」。行数以服务端 --stats 为准，不看客户端；
#   3) 每格从空目录起：否则第二格开始就在越长越大的树上插，几格之间不可比。
#
# 用法：bash benches/shard_write.sh
#       SECS=8 PER=16 SHARDS="1 2 4 8 16" bash benches/shard_write.sh
#       DUR=never bash benches/shard_write.sh      # 摘掉 fsync 只看 CPU 那层
#
# 全程只用临时目录与 127.0.0.1:17860..，不碰真数据。
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT" || exit 1
BIN=./target/release
BASE=${BASE:-17860}
SECS=${SECS:-5}
PER=${PER:-8}
PIPE=${PIPE:-8}
SHARDS=${SHARDS:-"1 2 4 8"}
DURS=${DUR:-"group never"}
OUT=benches/results/shard_write.log
R=.spot/shard
PIDS=""
fail=0
HZ=$(getconf CLK_TCK 2>/dev/null || echo 100)

cleanup() {
    local p
    for p in $PIDS; do kill -TERM "$p" 2>/dev/null; done
    for _ in $(seq 200); do
        local alive=0
        for p in $PIDS; do kill -0 "$p" 2>/dev/null && alive=1; done
        [ "$alive" = 0 ] && break
        sleep 0.05
    done
    for p in $PIDS; do kill -KILL "$p" 2>/dev/null; done
}
trap cleanup EXIT

say() { echo "$@" | tee -a "$OUT"; }
mkdir -p benches/results .spot
rm -rf "$R"; mkdir -p "$R"
: > "$R/raw.tsv"

# 服务端进程的 CPU 时间（utime+stime，时钟拍）。`comm` 里可能带空格，所以先剥掉
# 到最后一个 ')' 为止，再按剩余字段取第 12、13 个（原文件的 14、15）。
cpu_ticks() {
    local s rest
    s=$(cat "/proc/$1/stat" 2>/dev/null) || { echo 0; return; }
    rest=${s#*)}
    # shellcheck disable=SC2086
    set -- $rest
    echo $(( ${12:-0} + ${13:-0} ))
}

used_cores() { # TAG SECONDS_SPENT
    local tag=$1 wall=$2 t0 t1 sum=0 i
    for i in $(seq "$(cat "$R/$tag/nsrv")"); do
        t0=$(sed -n "${i}p" "$R/$tag/srvt0")
        t1=$(cpu_ticks "$(sed -n "${i}p" "$R/$tag/srvpids")")
        sum=$((sum + t1 - t0))
    done
    awk -v s="$sum" -v hz="$HZ" -v w="$wall" 'BEGIN { printf "%.2f", s / hz / w }'
}

up() { # PORT
    local i=0
    until (exec 3<>/dev/tcp/127.0.0.1/$1) 2>/dev/null; do
        i=$((i + 1)); [ $i -gt 400 ] && return 1; sleep 0.05
    done
}

rows_of() { # PORT
    $BIN/treedb-server --listen 127.0.0.1:$1 --stats 2>/dev/null \
        | grep -oE 'rows [0-9]+' | head -1 | cut -d' ' -f2
}

cell() { # TAG MODE S DUR
    local tag=$1 mode=$2 S=$3 dur=$4
    local nsrv=$S i bp t0 t1 ops=0 want=0 rows=0 bad=0
    [ "$mode" = solo ] && nsrv=1
    rm -rf "$R/$tag"; mkdir -p "$R/$tag"
    : > "$R/$tag/srvpids"; : > "$R/$tag/srvt0"
    echo "$nsrv" > "$R/$tag/nsrv"

    for i in $(seq "$nsrv"); do
        bp=$((BASE + i - 1))
        (exec 3<>/dev/tcp/127.0.0.1/$bp) 2>/dev/null && { say "!! 端口 $bp 被占"; exit 1; }
        $BIN/treedb-server --dir "$R/$tag/$i" --listen 127.0.0.1:$bp --workers 0 \
            --durability "$dur" --auto-checkpoint-mb 0 > "$R/$tag/srv$i.log" 2>&1 &
        echo "$!" >> "$R/$tag/srvpids"
        PIDS="$PIDS $!"
        echo "$(cpu_ticks $!)" >> "$R/$tag/srvt0"
        up "$bp" || { say "   !! $tag 第 $i 个服务端起不来：$(tail -2 "$R/$tag/srv$i.log")"; fail=$((fail + 1)); return; }
    done

    # 客户端：S 个进程，每个 PER 条连接、PER 条线程、各打一张自己的表。
    local bpid=""
    for i in $(seq "$S"); do
        if [ "$mode" = split ]; then bp=$((BASE + i - 1)); else bp=$BASE; fi
        TREEDB_BENCH_TABLE="sh$i" $BIN/treedb-bench --addr 127.0.0.1:$bp --op rowinsert \
            --conns "$PER" --threads "$PER" --pipeline "$PIPE" --secs "$SECS" \
            > "$R/$tag/b$i.out" 2>&1 &
        bpid="$bpid $!"
    done
    t0=$(date +%s.%N)
    local rc
    for pid in $bpid; do
        wait "$pid"; rc=$?
        [ "$rc" != 0 ] && bad=$((bad + 1))
    done
    t1=$(date +%s.%N)
    local wall
    wall=$(awk -v a="$t0" -v b="$t1" 'BEGIN { printf "%.1f", b - a }')

    for i in $(seq "$S"); do
        local l q sp
        l=$(tail -1 "$R/$tag/b$i.out")
        case "$l" in
            *"errors=0"*) ;;
            *) say "   !! $tag 第 $i 路有错误：$l"; bad=$((bad + 1)) ;;
        esac
        q=$(echo "$l" | grep -oE '→ [0-9]+ ops/s' | grep -oE '[0-9]+')
        [ -z "$q" ] && { say "   !! $tag 第 $i 路没出数字：$l"; bad=$((bad + 1)); q=0; sp=0; }
        # 这一路自己报的窗口长度：ops/s 的分母就是它，乘回去才是真的发出条数。
        # 不拿 cell 的 wall 当分母 —— 那里面还包括建连 + 建表的固定开销，每路
        # 差零点几秒，在 SECS=5 上就是百分之几，会把下面的行数闸门误伤。
        sp=${sp:-$(echo "$l" | grep -oE 'over [0-9.]+s' | grep -oE '[0-9.]+')}
        ops=$((ops + q))
        want=$((want + $(awk -v a="$q" -v b="${sp:-0}" 'BEGIN { printf "%.0f", a * b }')))
    done

    # 服务端侧的行数：客户端说跑了多少 op，树上就得真的多出多少行。
    for i in $(seq "$nsrv"); do
        local r
        r=$(rows_of $((BASE + i - 1)))
        rows=$((rows + ${r:-0}))
    done
    # rowinsert 全是 PUT_IF_ABSENT 的新键，行数应当等于发出的 op 数。取 ±2%：
    # 每路的 ops/s 是取整后再乘回窗长，加上统计本身的舍入，不会一分不差。
    local lo=$((want * 98 / 100)) hi=$((want * 102 / 100 + 1))
    if [ "$want" -gt 1000 ] && { [ "$rows" -lt "$lo" ] || [ "$rows" -gt "$hi" ]; }; then
        say "   !! $tag 行数对不上：算出 $want，服务端报 $rows（客户端合计 $ops ops/s）"
        bad=$((bad + 1))
    fi

    local cores
    cores=$(used_cores "$tag" "$wall")
    if [ "$bad" != 0 ]; then
        say "   !! $tag 有 $bad 处不合格，这一格不计入汇总"
        fail=$((fail + 1))
    else
        # 「每实例几核」是这一套的判据：solo 的单一服务端如果顶到了几个核而吞吐
        # 不涨，墙就不在 CPU 上；如果连 1 核都不到，那才是被单路提交线程卡住。
        say "$(printf '   %-10s %10d ops/s  每实例 %8d  服务端 %5s 核（每实例 %5s）  rows=%d' \
            "$mode/$S" "$ops" "$((ops / S))" "$cores" \
            "$(awk -v c="$cores" -v n="$nsrv" 'BEGIN { printf "%.2f", c / n }')" "$rows")"
        printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$dur" "$mode" "$S" "$ops" "$cores" "$wall" \
            >> "$R/raw.tsv"
    fi

    for pid in $PIDS; do kill -TERM "$pid" 2>/dev/null; done
    wait 2>/dev/null
    PIDS=""
}

say "== 多实例写扩展 $(date '+%F %T')  secs=$SECS per=$PER pipeline=$PIPE shards='$SHARDS' =="
for dur in $DURS; do
    say "-- durability=$dur"
    for s in $SHARDS; do
        cell "solo-$dur-$s" solo "$s" "$dur"
        cell "split-$dur-$s" split "$s" "$dur"
    done
done

# ---- 汇总：每格的 ops/s 除以本 durability 下 solo/1 那一格 = 扩展比 ----
say ""
say "== 汇总（相对 solo/1 的写扩展比）=="
awk -F'\t' '
{ v[$1 "/" $2 "/" $3] = $4; c[$1 "/" $2 "/" $3] = $5; if ($3 + 0 > mx[$1]) mx[$1] = $3 }
END {
  n = split("group never", ds, " ")
  for (i = 1; i <= n; i++) {
    d = ds[i]
    base = v[d "/solo/1"] + 0
    if (base == 0) continue
    printf "-- %s：solo/1 = %d ops/s\n", d, base
    printf "   %-6s %12s %12s %10s %10s %9s\n", \
      "实例", "solo 总ops", "solo 扩展", "split 总ops", "split 扩展", "split 核"
    m = split("1 2 4 8 16 32", ss, " ")
    for (j = 1; j <= m; j++) {
      s = ss[j]
      if (v[d "/solo/" s] + 0 == 0 || v[d "/split/" s] + 0 == 0) continue
      printf "   %-6s %12d %12.2f %12d %12.2f %9.2f\n", s, \
        v[d "/solo/" s], v[d "/solo/" s] / base, \
        v[d "/split/" s], v[d "/split/" s] / base, c[d "/split/" s]
    }
  }
}' "$R/raw.tsv" | tee -a "$OUT"

say ""
if [ "$fail" -eq 0 ]; then
    say "== 全部通过：每格 errors=0、服务端行数与发出的 op 数一致 =="
    exit 0
fi
say "== $fail 处失败 =="
exit 1
