#!/bin/bash
# grovefs 接入侧的 pin 跨代复核（§10 阶段 C 4 末、§11 第 21 条）。
#
# 回答一个问题：grovefs 的控制面迁移分支把 treedb 的 tag 往上升，要花多少改动？
# 做法是同一份源码、同一个 worktree、只改 Cargo.toml 那一行 tag，两臂各跑
# 「编译 / 仓内测试 / 六条端到端套件」，再加一档混代反证。
#
# 用法：bash benches/grovefs_pin_ab.sh [grovefs worktree] [该分支现有 pin]
#   默认 /data/grovefs/.wt-errno 与 v0.3.6。worktree 的 Cargo.toml/Cargo.lock
#   必须干净（脚本会临时改它们并在退出时还原），所以别拿主检出跑。
# 输出到 stdout，落盘在 benches/results/grovefs_pin_ab.log。
set -u
# 不把构建指到别处去：套件硬编码 ./target/release/grovefs，一旦环境里有
# CARGO_TARGET_DIR，两臂建的二进制就不在套件读的那个位置，跑的会是残留。
unset CARGO_TARGET_DIR

R=$(cd "$(dirname "$0")/.." && pwd)        # treedb 仓根
W=${1:-/data/grovefs/.wt-errno}
OLD=${2:-v0.3.6}
SUITES="test_treedb_control.sh test_treedb_matrix.sh test_treedb.sh \
test_locks_treedb.sh test_govcheck_treedb.sh test_govtime_treedb.sh"
T=$R/.pinab                                 # 旧引擎的隔离构建目录（gitignored）
NEW=$(git -C "$R" describe --tags --abbrev=0)
NEWH=$(git -C "$R" rev-parse --short HEAD)

die() { echo "ABORT: $*" >&2; exit 1; }
[ -d "$W/.git" ] || [ -f "$W/.git" ] || die "$W 不是一个 git worktree"
git -C "$W" diff --quiet Cargo.toml Cargo.lock || die "$W 的 Cargo.toml/Cargo.lock 有未提交改动"
grep -q "tag = \"$OLD\"" "$W/Cargo.toml" || die "$W 的 Cargo.toml 里没有 tag = \"$OLD\""
trap 'git -C "$W" checkout -- Cargo.toml Cargo.lock 2>/dev/null' EXIT
# 后面所有 cargo 调用跑的是 grovefs 那颗 worktree；仓内路径一律走绝对变量。
cd "$W" || die "进不去 $W"

# 只改 tag 那一行；lock 交给 cargo 自己重解析
setpin() { sed -i "s#tag = \"$1\"#tag = \"$2\"#" "$W/Cargo.toml"; }
pin() { grep -m1 '^treedb = { git' "$W/Cargo.toml" | sed 's/.*tag = "\([^"]*\)".*/\1/'; }
# 汇总 cargo test 的全部 "test result:" 行（字段带分号，先剔掉再比）
agg() { awk '{gsub(/;/,"")} /^test result:/ {for(i=1;i<=NF;i++)
           if($(i+1)=="passed"||$(i+1)=="failed"||$(i+1)=="ignored") s[$(i+1)]+=$i}
         END {printf "passed=%d failed=%d ignored=%d\n", s["passed"], s["failed"], s["ignored"]}' "$1"; }

echo "===== grovefs 迁移分支 × 两个引擎 pin 的跨代复核（新 pin $NEW / 引擎 $NEWH）====="
echo "# worktree $W @ $(git -C "$W" rev-parse --short HEAD)（$(git -C "$W" rev-parse --abbrev-ref HEAD)）"
echo "# 唯一变量 = Cargo.toml 的 tag 行；两臂不共用引擎二进制，否则测的是"
echo "# 「新引擎兼容旧客户端」而不是「pin 变了」。"
echo
echo "----- 0) grovefs 侧各 ref 的现状（pin 只读 Cargo.toml 那一行）-----"
G=$(dirname "$(git -C "$W" rev-parse --path-format=absolute --git-common-dir)")
for ref in master origin/master feat/treedb-hardlink origin/feat/treedb-hardlink \
           feat/treedb-read-errno origin/feat/treedb-read-errno; do
  p=$(git -C "$G" show "$ref:Cargo.toml" 2>/dev/null | grep -m1 '^treedb = { git' | sed 's/.*tag = "\([^"]*\)".*/\1/')
  cm=$(git -C "$G" cat-file -e "$ref:src/control_migrate.rs" 2>/dev/null && echo 有 || echo 无)
  hk=$(git -C "$G" grep -l 'Op::Link' "$ref" -- src 2>/dev/null | head -1)
  [ -n "$hk" ] && link="有(${hk#*:})" || link=无
  printf '  %-30s pin=%-8s control_migrate=%s  Op::Link=%s\n' "$ref" "${p:-n/a}" "$cm" "$link"
done
git -C "$G" rev-list --left-right --count "origin/master...master" \
  | awk '{printf "  本地 master 与 origin/master 分叉：落后 %s / 独有 %s\n", $1, $2}'
echo "  主检出未提交文件 $(git -C "$G" status --porcelain | grep -c .) 个"
git -C "$W" grep -oh '"grovefs_control__[a-z_]*"' -- src | tr -d '"' | sort -u \
  | tr '\n' ' ' | sed 's/^/  写进引擎的表：/; s/$/\n/'
for op in Key Audit Def Place; do
  echo "  旧专用 op Op::${op}* 在该分支 src 里的残留：$(git -C "$W" grep -c "Op::${op}" -- src 2>/dev/null | awk -F: '{s+=$NF} END {print s+0}') 行"
done

echo
echo "----- 1) 只改 tag 一行：cargo check --release --features treedb --all-targets -----"
# 编译/测试臂用隔离 target，不碰 $W/target/release —— 端到端套件硬编码
# Y=./target/release/grovefs，一旦共用了目录，套件跑的可能是上一臂的残留二进制。
TD="--target-dir $W/target_pinab"
for arm in A B; do
  tag=$([ "$arm" = A ] && echo "$OLD" || echo "$NEW")
  [ "$arm" = B ] && setpin "$OLD" "$NEW"
  echo "# 臂 $arm：pin $(pin)"
  out=$(cargo check --release --features treedb --all-targets $TD 2>&1); rc=$?
  echo "$out" | grep -E '^(warning|error)' | head -3
  echo "  $(echo "$out" | grep -m1 'Finished' | tr -s ' ')  rc=$rc"
  echo "  warning=$(echo "$out" | grep -c '^warning:') error=$(echo "$out" | grep -c '^error:')"
  git -C "$W" diff --stat Cargo.toml Cargo.lock | tail -1 | sed 's/^/  /'
done

echo
echo "----- 2) cargo test --release --features treedb（同一棵树、同两个 pin）-----"
for arm in B A; do
  tag=$([ "$arm" = A ] && echo "$OLD" || echo "$NEW")
  [ "$arm" = A ] && git -C "$W" checkout -- Cargo.toml Cargo.lock
  echo "# 臂 $arm：pin $(pin)"
  mkdir -p "$T"
  cargo test --release --features treedb $TD > "$T/test-$arm.log" 2>&1; rc=$?
  echo "  $(agg "$T/test-$arm.log")  rc=$rc"
done
git -C "$W" checkout -- Cargo.toml Cargo.lock

echo
echo "----- 3) 六条端到端套件 × 两臂（引擎二进制各自同代）-----"
mkdir -p "$T"
[ -d "$T/old/src" ] || (mkdir -p "$T/old" && git -C "$R" archive "$OLD" | tar -x -C "$T/old")
[ -x "$R/target/release/treedb-server" ] || cargo build --release --manifest-path "$R/Cargo.toml" --bins
BINNEW=$R/target/release/treedb-server
if [ ! -x "$T/oldbin/treedb-server" ]; then
  cargo build --release --manifest-path "$T/old/Cargo.toml" --bins --target-dir "$T/oldtarget" 2>&1 | tail -1
  mkdir -p "$T/oldbin" && cp "$T/oldtarget/release/treedb-server" "$T/oldbin/"
fi
sha256sum "$BINNEW" "$T/oldbin/treedb-server" | sed 's/^/  /'
for arm in B A; do
  tag=$([ "$arm" = A ] && echo "$OLD" || echo "$NEW")
  if [ "$arm" = A ]; then git -C "$W" checkout -- Cargo.toml Cargo.lock; TB=$T/oldbin/treedb-server; else setpin "$OLD" "$NEW"; TB=$BINNEW; fi
  # 每臂一整套隔离 target：套件硬编码 ./target/release/grovefs，共用的话 cargo 的
  # 新鲜判定会把上一臂的产物留着不动，两臂就在跑同一份二进制。建完再拷过去，
  # 并拷前把两臂的 sha256 印出来 —— 两个哈希不同本身就是「两臂确实是两份构建」的证据。
  cargo build --release --features treedb --bins --target-dir "$T/tgt-$arm" 2>&1 | tail -1 | sed 's/^/  build: /'
  sha256sum "$T/tgt-$arm/release/grovefs" | sed 's/^/  grovefs /'
  mkdir -p "$W/target/release" && cp -f "$T/tgt-$arm/release/grovefs" "$W/target/release/grovefs"
  ls -l --time-style=+%H:%M:%S "$W/target/release/grovefs" | awk '{print "  套件用的二进制：", $5, "B  mtime", $6}'
  echo "# 臂 $arm：pin $(pin) / 引擎 $TB"
  for s in $SUITES; do
    TREEDB_BIN=$TB bash "$W/.build-bin/$s" > "$T/$arm-$s.log" 2>&1; rc=$?
    echo "  $(grep -m1 'SUMMARY' "$T/$arm-$s.log")  [$s rc=$rc]"
  done
done
git -C "$W" checkout -- Cargo.toml Cargo.lock

echo
echo "----- 4) 混代反证：旧 pin 的 grovefs 接新引擎二进制 -----"
echo "# 这正是套件里 TREEDB_BIN 未设时的默认取值（\$PWD/../treedb/target/release/treedb-server）。"
for s in test_treedb_control.sh test_treedb_matrix.sh; do
  TREEDB_BIN=$BINNEW bash "$W/.build-bin/$s" > "$T/mix-$s.log" 2>&1; rc=$?
  echo "  $(grep -m1 'SUMMARY' "$T/mix-$s.log")  [$s rc=$rc]"
  grep -m1 'errno 68' "$T/mix-$s.log" | sed 's/^/  /'
done
echo
echo "----- 5) pin 锁定的 rev 与 treedb HEAD 的差 -----"
echo "  tag $NEW = 标注对象 $(git -C "$R" rev-parse --short "$NEW") → commit $(git -C "$R" rev-parse --short "$NEW^{commit}")"
echo "  仓 HEAD $NEWH 在 tag 之后 $(git -C "$R" rev-list --count "$NEW^{commit}..HEAD") 笔（tag 锁定的构建不含它们）："
git -C "$R" log --oneline "$NEW^{commit}..HEAD" | sed 's/^/    /'
