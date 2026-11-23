#!/bin/bash
# 快照升级的真机彩排：拿**上一个 tag 真编译出来的旧二进制**造一份 v3 数据目录，
# 用当前树的 `treedb-migrate` 升上去，再把四条承诺逐条量一遍。
#
# 为什么要有这个脚本，`tests/migrate.rs` 十几条用例不够吗：不够。那边的 v3 样本是
# `save_v3()` 写的，而 `save_v3()` 与迁移工具是同一轮加的 —— 它只证明「代码自洽」，
# 不证明「真的能接住 v0.5.1 落盘的那种文件」。万一 v0.5.1 的写出侧与 `save_v3`
# 有任何一处不一致（字段顺序、某个计数、尾巴 seq 的位置），单测会全绿而线上开门就崩。
# 这里换的是外部效度，不是重复内部效度。
#
# 顺带量两条只在真机上才看得见的事：
#   1) 迁移前后 `treedb.wal` 的 sha256 必须逐字节相同，备份的 sha256 必须等于原件；
#   2) 快照尺寸的差必须**恰好 -24 字节** = 正文 `refs` 之后那段计数被抽掉
#      （4×u32 四张表计数 + u64 审计序号）；v3 与 v4 的尾部 16 字节尾巴同形。
#      这条比「v4 比 v3 小」强得多：正文一旦多抽或少抽就会先在这里炸。
#
# 还演示回滚窗口的真实边界（§6.5 末）：只要新二进制还没做过第一次 checkpoint，
# 把备份拷回去跑旧二进制就是无损的（那几条写还在 WAL 里）；一旦 checkpoint 过，
# `wal.reset()` 把上一代记录物理抹掉，此后回滚只能回到快照那一刻。
#
# 用法：
#   benches/rehearse_migrate.sh [基线 tag=v0.5.1]
# 前置：`cargo build --release` 已跑过（当前工作树的二进制当 new 档）。
# 结果追加到 benches/results/rehearse_migrate.log；临时目录在 .matrix/mig（已 gitignore）。
set -u
cd "$(dirname "$0")/.." || exit 1
BASE=${1:-v0.5.1}
ROOT=$(pwd)
SRC=$ROOT/.matrix/mig/$BASE
DATA=$ROOT/.matrix/mig/data
OUT=benches/results/rehearse_migrate.log
OLD_PORT=${OLD_PORT:-17861}
NEW_PORT=${NEW_PORT:-17862}
SECS=${SECS:-2}
mkdir -p "$ROOT/.matrix/mig" benches/results "$(dirname "$DATA")"

NEW_SRV=$ROOT/target/release/treedb-server
NEW_BENCH=$ROOT/target/release/treedb-bench
NEW_MIG=$ROOT/target/release/treedb-migrate
OLD_SRV=$SRC/target/release/treedb-server
OLD_BENCH=$SRC/target/release/treedb-bench

fail=""
note() { printf '%s\n' "$*" | tee -a "$OUT"; }
need() { # need <标签> <期望> <实际>
    if [ "$2" = "$3" ]; then note "ok   $1 = $3"; else note "FAIL $1: 期望 $2, 实得 $3"; fail="$1"; fi
}
# stats <二进制> <端口> <字段名>：从 --stats 输出里取一个整数
stats() { $1 --listen 127.0.0.1:$2 --stats 2>/dev/null | awk -F'= *' -v k="$3" '$1 ~ k {print $2+0}'; }
# 快照文件尾的水位（seq）。新版 16 字节尾巴（crc32 u32 | seq u64 | magic u32）取
# 倒数 12..4；旧版 8 字节尾巴（seq u64）就是最后 8 字节，靠 magic 分辨。直接读字节
# 而不读 `--stats`：一来 `--stats` 在优雅停服前读到的是内存里的 seq，二来拿文件
# 字节当参照才配得上「尾巴照搬」这句话。
snaptrailer() {
    local magic
    magic=$(tail -c 4 "$1" | od -An -tx4 | tr -d ' ')
    if [ "$magic" = "31534254" ]; then
        tail -c 12 "$1" | head -c 8 | od -An -tu8 | tr -d ' '
    else
        tail -c 8 "$1" | od -An -tu8 | tr -d ' '
    fi
}
sha() { sha256sum "$1" | cut -c1-16; }
stop() { $1 --listen 127.0.0.1:$2 --shutdown >/dev/null 2>&1; sleep 1; }
killport() { pkill -9 -f "listen 127.0.0.1:$1" 2>/dev/null; sleep 1; }

[ -x "$NEW_SRV" ] || { echo "先 cargo build --release"; exit 2; }
[ -x "$NEW_MIG" ] || { echo "缺 target/release/treedb-migrate"; exit 2; }

note "---- 彩排 $BASE → 当前树（$(date -Is)）----"

# 1) 旧二进制造一份真的 v3 数据目录。`--auto-checkpoint-mb 1` 是为了让快照真的落盘：
#    0 是「关掉自动 checkpoint」（代码里有 >0 的闸门），不是「每条都刷」。
if [ ! -x "$OLD_SRV" ]; then
    mkdir -p "$SRC"
    git archive "$BASE" | tar -x -C "$SRC" || { note "git archive $BASE 失败"; exit 2; }
    (cd "$SRC" && cargo build --release --bin treedb-server --bin treedb-bench >/dev/null 2>&1) || {
        note "旧二进制编译失败"; exit 2; }
fi
rm -rf "$DATA"; mkdir -p "$DATA"
nohup "$OLD_SRV" --dir "$DATA" --listen 127.0.0.1:$OLD_PORT --auto-checkpoint-mb 1 \
    >"$ROOT/.matrix/mig/old.log" 2>&1 &
sleep 2
"$OLD_BENCH" --addr 127.0.0.1:$OLD_PORT --op create --conns 4 --secs "$SECS" --pipeline 8 >/dev/null 2>&1
"$OLD_BENCH" --addr 127.0.0.1:$OLD_PORT --op write --conns 4 --secs "$SECS" --pipeline 8 >/dev/null 2>&1
V_OLD=$(stats "$OLD_SRV" "$OLD_PORT" views)
N_OLD=$(stats "$OLD_SRV" "$OLD_PORT" nodes)
D_OLD=$(stats "$OLD_SRV" "$OLD_PORT" dentries)
stop "$OLD_SRV" "$OLD_PORT"
SNAP_VER=$(head -c 4 "$DATA/state.snap" | od -An -tu4 | tr -d ' ')
[ "$SNAP_VER" = "3" ] || { note "旧目录的快照版本位是 $SNAP_VER，不是 3 —— 彩排前提没了"; exit 2; }
SHA_WAL0=$(sha "$DATA/treedb.wal"); SHA_SNAP0=$(sha "$DATA/state.snap")
SZ0=$(stat -c %s "$DATA/state.snap")
S_OLD=$(snaptrailer "$DATA/state.snap")
note "旧二进制：views=$V_OLD nodes=$N_OLD dentries=$D_OLD snap_seq=$S_OLD snap=$SZ0 B（版本位 $SNAP_VER）"

# 2) 只读体检必须先给 exit 10，且一个字节都不写
"$NEW_MIG" --dir "$DATA" >"$ROOT/.matrix/mig/check.log" 2>&1
need "不带 --apply 的退出码" 10 "$?"
need "体检不写任何文件" "$SHA_SNAP0" "$(sha "$DATA/state.snap")"

# 3) 真迁移
"$NEW_MIG" --dir "$DATA" --apply >"$ROOT/.matrix/mig/apply.log" 2>&1
need "--apply 退出码" 0 "$?"
need "WAL 逐字节没动" "$SHA_WAL0" "$(sha "$DATA/treedb.wal")"
# 备份名 `state.snap.v2.bak` 是历史（v2 → v3 那一轮起的），故意不改：回滚脚本
# 与既有备份都按这个名字找文件。
need "备份 = 原件" "$SHA_SNAP0" "$(sha "$DATA/state.snap.v2.bak")"
SZ1=$(stat -c %s "$DATA/state.snap")
need "快照尺寸差 = -24 B（抽掉四段计数）" "$((SZ0 - 24))" "$SZ1"
note "apply 输出：$(grep -E '^migrated|^backup|^wal ' "$ROOT/.matrix/mig/apply.log" | tr '\n' ' | ')"

# 4) 新二进制开门，计数必须与旧二进制逐字段相同
nohup "$NEW_SRV" --dir "$DATA" --listen 127.0.0.1:$NEW_PORT >"$ROOT/.matrix/mig/new.log" 2>&1 &
sleep 2
V_NEW=$(stats "$NEW_SRV" "$NEW_PORT" views)
N_NEW=$(stats "$NEW_SRV" "$NEW_PORT" nodes)
D_NEW=$(stats "$NEW_SRV" "$NEW_PORT" dentries)
need "迁移后 views" "$V_OLD" "$V_NEW"
need "迁移后 nodes" "$N_OLD" "$N_NEW"
need "迁移后 dentries" "$D_OLD" "$D_NEW"
S_MIG=$(snaptrailer "$DATA/state.snap")
need "快照水位照搬（尾巴 8 字节逐字节相等）" "$S_OLD" "$S_MIG"

# 5) 回滚窗口 A：新二进制写过、但**没 checkpoint**（直接 kill -9），此时回滚无损。
"$NEW_BENCH" --addr 127.0.0.1:$NEW_PORT --op create --conns 4 --secs "$SECS" --pipeline 8 >/dev/null 2>&1
N_LIVE=$(stats "$NEW_SRV" "$NEW_PORT" nodes)
killport "$NEW_PORT"
cp "$DATA/state.snap.v2.bak" "$DATA/state.snap"
nohup "$OLD_SRV" --dir "$DATA" --listen 127.0.0.1:$OLD_PORT >"$ROOT/.matrix/mig/rb.log" 2>&1 &
sleep 3
N_RB=$(stats "$OLD_SRV" "$OLD_PORT" nodes)
stop "$OLD_SRV" "$OLD_PORT"
need "WAL 完好时回滚无损（旧二进制读到迁移后的全部写）" "$N_LIVE" "$N_RB"

# 6) 回滚窗口 B：新二进制 checkpoint 过一次（`--shutdown` 收尾就是一次
#    checkpoint + `wal.reset()`），此后回滚只能回到快照那一刻 —— 那之后的写已经
#    不在 WAL 里了。这一格**期望的就是「有损」**，它把 §6.5 那条边界钉成一个数字。
nohup "$NEW_MIG" --dir "$DATA" --apply --force >/dev/null 2>&1   # 上一格把 snap 换回了 v3
nohup "$NEW_SRV" --dir "$DATA" --listen 127.0.0.1:$NEW_PORT >"$ROOT/.matrix/mig/new2.log" 2>&1 &
sleep 2
"$NEW_BENCH" --addr 127.0.0.1:$NEW_PORT --op create --conns 4 --secs "$SECS" --pipeline 8 >/dev/null 2>&1
N_BEFORE_CK=$(stats "$NEW_SRV" "$NEW_PORT" nodes)
stop "$NEW_SRV" "$NEW_PORT"            # 优雅停服 = checkpoint + wal.reset
cp "$DATA/state.snap.v2.bak" "$DATA/state.snap"
"$NEW_MIG" --dir "$DATA" --apply --force >/dev/null 2>&1
nohup "$NEW_SRV" --dir "$DATA" --listen 127.0.0.1:$NEW_PORT >"$ROOT/.matrix/mig/new3.log" 2>&1 &
sleep 2
N_AFTER_CK=$(stats "$NEW_SRV" "$NEW_PORT" nodes)
stop "$NEW_SRV" "$NEW_PORT"
if [ "$N_AFTER_CK" -lt "$N_BEFORE_CK" ]; then
    note "ok   checkpoint 之后回滚确实丢写：$N_BEFORE_CK → $N_AFTER_CK（回滚窗口 = 第一次 checkpoint 之前）"
else
    note "FAIL checkpoint 之后回滚没有丢写（$N_BEFORE_CK → $N_AFTER_CK）—— §6.5 那条边界的描述要重写"
    fail="rollback-window"
fi

note "---- 结论：$([ -z "$fail" ] && echo "四条承诺全部对上真机数据" || echo "有格失败：$fail") ----"
[ -z "$fail" ] || exit 1
