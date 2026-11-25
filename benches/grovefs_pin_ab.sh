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
echo "----- 3) 六条端到端套件 × 两臂（引擎二进制与各自 pin 的 tag 同源）-----"
mkdir -p "$T"
# 两臂的引擎都从**各自那个 tag 的源码**现编，不拿 $R/target 里的 HEAD 产物：臂 B 用
# HEAD 建的服务端，测的就是「旧 tag 的客户端 + tag 之后的服务端」，与「各配同代引擎」
# 这句话不符（差的那几笔里恰好有服务端侧的锁修补）。
proto_of() {   # 读 stdin，取 PROTOCOL_VERSION 的**数值**：`tr -dc 0-9` 会把类型名
  # u16 里的 16 一起留下（曾经印成 proto=168），那不是一个能拿来对照代的信息。
  sed -n 's/.*PROTOCOL_VERSION: u16 = \([0-9]\{1,\}\).*/\1/p' | head -1
}
mkeng() {  # mkeng <tag> → 只吐一个引擎路径（诊断一律走 stderr，不然会被读进变量）
  local g=$1 d b
  d=$T/src-$g; b=$T/bin-$g/treedb-server   # 别写成 `local d=$T/src-$g`：local 的赋值词
  # 是先整体展开再赋值的，同一句里引用前一个变量会撞上 set -u，整个函数当场空手而归
  [ -d "$d/src" ] || (mkdir -p "$d" && git -C "$R" archive "$g" | tar -x -C "$d")
  if [ ! -x "$b" ]; then
    cargo build --release --manifest-path "$d/Cargo.toml" --bins \
      --target-dir "$d/target" 2>&1 | tail -1 | sed 's/^/  build /' >&2
    mkdir -p "$T/bin-$g" && cp -f "$d/target/release/treedb-server" "$T/bin-$g/"
  fi
  [ -x "$b" ] || { echo "ABORT: 从 $g 建不出引擎 $b" >&2; exit 1; }
  echo "$b"
}
BINOLD=$(mkeng "$OLD")
BINNEW=$(mkeng "$NEW")
# 这两道闸是必须的：$TB 一旦为空，套件里的 `${TREEDB_BIN:-$PWD/../treedb/target/…}`
# 会静默退回默认路径，整块就悄悄变成了「两臂共用 HEAD 引擎」——正是本块要排除的那种测法。
[ -x "$BINOLD" ] || die "臂 A 的引擎不可执行：'$BINOLD'（$OLD 的构建没成）"
[ -x "$BINNEW" ] || die "臂 B 的引擎不可执行：'$BINNEW'（$NEW 的构建没成）"
engline() {   # 每一代引引擎的 rev / sha / 协议号：三份证据各自独立可读
  printf '  引擎 %-8s rev=%-8s sha=%-16s proto=%s\n' "$1" \
    "$(git -C "$R" rev-parse --short "$1^{commit}")" \
    "$(sha256sum "$2" | cut -c1-16)" \
    "$(git -C "$R" show "$1:src/engine/mod.rs" | proto_of)"
}
engline "$OLD" "$BINOLD"
engline "$NEW" "$BINNEW"
for arm in B A; do
  tag=$([ "$arm" = A ] && echo "$OLD" || echo "$NEW")
  if [ "$arm" = A ]; then git -C "$W" checkout -- Cargo.toml Cargo.lock; TB=$BINOLD; else setpin "$OLD" "$NEW"; TB=$BINNEW; fi
  [ -x "$TB" ] || die "臂 $arm 的引擎为空：'$TB'"
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
echo "----- 4) 混代反证：旧 pin 的 grovefs 接本仓 HEAD 建出的引擎 -----"
echo "# 这正是套件里 TREEDB_BIN 未设时的默认取值（\$PWD/../treedb/target/release/treedb-server）："
# 拿默认路径上那份真去跑，而不是拿臂 B 的 tag 构建——要证的恰好是「默认值会指到谁」。
cargo build --release --manifest-path "$R/Cargo.toml" --bins 2>&1 | tail -1 | sed 's/^/  build /' >&2
BINHEAD=$R/target/release/treedb-server
printf '# HEAD 引擎 rev=%s sha=%s proto=%s\n' "$NEWH" \
  "$(sha256sum "$BINHEAD" | cut -c1-16)" \
  "$(proto_of < "$R/src/engine/mod.rs")"
for s in test_treedb_control.sh test_treedb_matrix.sh; do
  TREEDB_BIN=$BINHEAD bash "$W/.build-bin/$s" > "$T/mix-$s.log" 2>&1; rc=$?
  echo "  $(grep -m1 'SUMMARY' "$T/mix-$s.log")  [$s rc=$rc]"
  grep -m1 'errno 68' "$T/mix-$s.log" | sed 's/^/  /'
done
echo
echo "----- 5) pin 锁定的 rev 与 treedb HEAD 的差 -----"
echo "  tag $NEW = 标注对象 $(git -C "$R" rev-parse --short "$NEW") → commit $(git -C "$R" rev-parse --short "$NEW^{commit}")"
echo "  仓 HEAD $NEWH 在 tag 之后 $(git -C "$R" rev-list --count "$NEW^{commit}..HEAD") 笔（tag 锁定的构建不含它们）："
git -C "$R" log --oneline "$NEW^{commit}..HEAD" | sed 's/^/    /'
