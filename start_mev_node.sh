#!/usr/bin/env bash
# =============================================================================
#  sui 全节点启动脚本（MEV 补丁版，分支 mev-patch）
# =============================================================================
#
#  用法：
#      ./start_mev_node.sh build     # 1. 先编译
#      ./start_mev_node.sh check     # 2. 只做启动前自检，不启动（含协议版本预检）
#      ./start_mev_node.sh start     # 3. 前台启动（Ctrl-C 停止，调试用）
#      ./start_mev_node.sh daemon    # 3'. 后台启动，日志写 $LOG_FILE
#      ./start_mev_node.sh verify    # 4. 启动后验证补丁是否真的生效
#      ./start_mev_node.sh stop      # 5. 停止 daemon 模式启动的节点
#
#  ---------------------------------------------------------------------------
#  为什么需要这个脚本：补丁的开关分两类，走两条完全不同的路，不能混
#  ---------------------------------------------------------------------------
#
#  A) 两个 Unix socket —— 只能写在 YAML 里，且必须 kebab-case
#     NodeConfig 的加载就是 serde_yaml::from_reader，没有 env 覆盖层；
#     sui-node 的 CLI 只有 --config-path / --listen-address /
#     --run-with-range-* 四个参数，没有任何 mev 相关 flag。
#
#         mev-cache-update-socket: /tmp/sui_cache_updates.sock
#         mev-tx-socket:           /tmp/sui_tx.sock
#
#     ⚠ 致命坑：NodeConfig 没有 deny_unknown_fields。字段名拼错、或者按
#       Rust 习惯写成 snake_case（mev_cache_update_socket），**不会报错**，
#       会被静默忽略 —— 节点照常同步、照常出 RPC，只是推送功能完全没开。
#       本脚本的 check/verify 就是专门用来抓这个的。
#
#  B) 三个环境变量 —— 只能走 env，写进 YAML 完全无效
#     它们是代码里 std::env::var 直接读的，NodeConfig 里没有对应字段。
#
#     1. SUI_RECORD_POOL_RELATED_IDS
#        学习总开关。这是**唯一**会往内存集合 insert 新 id 的路径
#        （pool_related.rs 的 record()），所以：
#          关掉 ≠ 只是不写文件，而是**新池子永远不会被推送**。
#        ⇒ 生产环境请全程保持 =1，不要"预热完就关"。
#        成本可忽略：每个新 id 一次无缓冲 append 系统调用，且先判重。
#        真值判定：除 ""/0/false/no/off（忽略大小写与首尾空格）外都算真。
#
#     2. SUI_POOL_RELATED_IDS_PATH
#        id 清单路径。不设则 $HOME/sui/pool_related_ids.txt。
#        本脚本按你的要求固定为 ${POOL_IDS_PATH}。
#        ⚠ 该文件**每进程只读一次**（OnceLock）。运行中改文件内容无效，
#          必须重启节点。但运行中新学到的 id 会同时进内存和文件，即时生效。
#        ⚠ 格式：一行一个裸 ObjectID（0x + 64 hex）。老 relay-patch 的
#          id,related_id 逗号 CSV 在这里会被当成坏行 warn 掉，一条都加载不了。
#
#     3. SUI_MEV_WATCHED_OWNERS
#        逗号分隔地址，按 Owner::AddressOwner 匹配，与 id 清单是**并集**，
#        不是二选一。冷启动阶段建议**不设**（要的是把热池子都学出来，
#        白名单只会缩小范围）；等清单养肥、确认推送量过大需要收窄时再加。
#
#  ---------------------------------------------------------------------------
#  与 bot 侧（sui-mev）的对接约定
#  ---------------------------------------------------------------------------
#
#  方向：**节点 bind/listen，bot connect**。所以必须先起节点、再起 bot。
#  bot 起早了会连不上 socket 并 panic（collector.rs 的 reconnect expect）。
#
#  本脚本刻意把 socket 命名成 bot 侧的默认值，这样 bot 不用传任何 socket 参数：
#      节点 mev-cache-update-socket  <->  bot --update-cache-socket
#                                      (默认 /tmp/sui_cache_updates.sock)
#      节点 mev-tx-socket            <->  bot --tx-socket-path
#                                      (默认 /tmp/sui_tx.sock)
#
#  bot 侧仍需显式传这三个（它的默认值是 /home/ubuntu/...，与本部署不符）：
#      --db-path       <YAML 的 db-path>/live/store
#      --config-path   与节点同一份 fullnode.yaml（bot 会读它拿 genesis）
#      --preload-path  与 $POOL_IDS_PATH 同一个文件
#  check 子命令会把这条完整命令打印出来，直接复制即可。
#
#  注意：/tmp 会在重启后清空，socket 文件随之消失 —— 这是正常的，节点重启
#  会重新 bind（bind 前会主动删掉上次遗留的同名文件，否则 EADDRINUSE）。
#
#  ---------------------------------------------------------------------------
#  端到端状态提醒
#  ---------------------------------------------------------------------------
#
#  "节点真的推 → bot 真的收到并 reload" 这条链路**尚未做过端到端验证**
#  （见 docs/SUI_NODE_PATCH_IMPLEMENTATION.md §7 遗留项 1）。本脚本的 verify
#  只能证明"节点侧 bind 成功、清单在加载"，不能证明 bot 收到了正确数据。
#
#  ---------------------------------------------------------------------------
#  维护约定（改本脚本前先看）
#  ---------------------------------------------------------------------------
#
#  ⚠ 所有紧跟中文/全角标点（括号、逗号、冒号等）的变量展开，**必须写 ${VAR}**。
#    不加大括号时，在 UTF-8 locale（你的终端就是）下 bash 会把全角标点的首字节
#    当成变量名的一部分，报 "VAR<byte>: unbound variable" 直接中断（set -u）。
#    在 LC_CTYPE=C 下则不会报错 —— 所以这类 bug 很容易漏测。
#    自检方法：
#      perl -ne 'print "$.: $_" if /\$[A-Za-z_]\w*[\x80-\xff]/' start_mev_node.sh
#      应输出空；改完再跑：LC_ALL=en_US.UTF-8 ./start_mev_node.sh check
#
# =============================================================================

set -euo pipefail

# --- 路径与开关：全部可用环境变量覆盖，命令行不用改脚本 -------------------

# 脚本所在目录即 sui 仓库根目录
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 编译产物。必须先 build，否则 check 会直接失败并给出命令。
NODE_BIN="${NODE_BIN:-$REPO_ROOT/target/release/sui-node}"

# 节点配置文件。注意 bot 的 --config-path 要指向同一份。
CONFIG_PATH="${CONFIG_PATH:-$REPO_ROOT/fullnode.yaml}"

# 池子相关对象 id 清单：节点学习写入 + bot 预加载读取，两端共用这一个文件。
POOL_IDS_PATH="${POOL_IDS_PATH:-/Volumes/superfs/suidata/db/pool_related_ids.txt}"

# 学习总开关。见文件头 “B) 三个环境变量” 第 1 条：关掉后新池子永不推送，生产保持 1。
RECORD_POOL_IDS="${RECORD_POOL_IDS:-1}"

# 地址白名单，逗号分隔。留空 = 不设该变量（推荐冷启动时留空）。
WATCHED_OWNERS="${WATCHED_OWNERS:-}"

# 两个推送 socket。名字必须与 YAML 里的值、以及 bot 侧默认值三方一致。
SOCKET_CACHE="${SOCKET_CACHE:-/tmp/sui_cache_updates.sock}"
SOCKET_TX="${SOCKET_TX:-/tmp/sui_tx.sock}"

# daemon 模式的日志与 pid 文件
LOG_FILE="${LOG_FILE:-/tmp/sui-node-mev.log}"
PID_FILE="${PID_FILE:-/tmp/sui-node-mev.pid}"

# 协议版本比对用的 JSON-RPC 端点，默认不联网（置空）。
# 公共 fullnode 已废弃 JSON-RPC（suix_* 会回 -32601），想要主动比对就指向
# 一个自己可控的端点，例：PROTOCOL_RPC=http://127.0.0.1:9000（本节点自己的 RPC）。
PROTOCOL_RPC="${PROTOCOL_RPC:-}"
PROTOCOL_RPC_TIMEOUT="${PROTOCOL_RPC_TIMEOUT:-8}"

# YAML 里的顶级字段名（kebab-case，不是 snake_case）
YKEY_CACHE="mev-cache-update-socket"
YKEY_TX="mev-tx-socket"

# --- 输出helper -----------------------------------------------------------

ok()   { printf '  \033[32mOK\033[0m    %s\n' "$*"; }
warn() { printf '  \033[33mWARN\033[0m  %s\n' "$*"; }
bad()  { printf '  \033[31mFAIL\033[0m  %s\n' "$*"; }
die()  { bad "$*"; echo; echo "启动中止。"; exit 1; }

# 读 YAML 顶级字段的标量值：去注释、去引号、去首尾空格
yaml_top_value() {
  awk -v k="$1" '
    index($0, k ":") == 1 {
      sub("^" k ":[[:space:]]*", "")
      sub(/[[:space:]]*#.*$/, "")
      gsub(/^[ \t"]+|[ \t"]+$/, "")
      print
      exit
    }
  ' "$CONFIG_PATH"
}

# 找出重复的顶级键。serde_yaml 对重复字段不是警告而是直接失败，
# NodeConfig::load(...).unwrap() 会 panic：
#   called `Result::unwrap()` on an `Err` value: duplicate field `state-archive-read-config`
# 这个只在节点真正启动时才暴露，等于白跑一次，所以必须在自检阶段拦下。
# 只匹配顶格、非注释、非列表项的行；缩进的子字段和 - 项不算顶级键。
duplicate_top_level_keys() {
  awk '
    /^[A-Za-z][A-Za-z0-9_-]*:/ {
      key = $0
      sub(/:.*/, "", key)
      count[key]++
      where[key] = where[key] ? where[key] "、" NR : NR
    }
    END { for (k in count) if (count[k] > 1) print k "\t" count[k] "\t" where[k] }
  ' "$CONFIG_PATH"
}

# 读嵌在父字段下的标量（如 genesis: 里的 genesis-file-location）。
# 与上面分开是故意的：MEV 那两个字段必须顶格，用严格版才能把“误写进嵌套块”查出来。
yaml_nested_value() {
  awk -v k="$1" '
    {
      line = $0
      sub(/^[[:space:]]+/, "", line)
      if (index(line, k ":") == 1) {
        sub("^" k ":[[:space:]]*", "", line)
        sub(/[[:space:]]*#.*$/, "", line)
        gsub(/^[ \t"]+|[ \t"]+$/, "", line)
        print line
        exit
      }
    }
  ' "$CONFIG_PATH"
}

# --- 编译 -----------------------------------------------------------------

do_build() {
  echo "==> 编译 sui-node（release）"
  echo "    工具链由 $REPO_ROOT/rust-toolchain.toml 钉住（当前 1.96.1），rustup 会自动选。"
  echo "    首次全量编译在这个仓库规模下要几十分钟，别以为卡住了。"
  cd "$REPO_ROOT"
  cargo build --release --bin sui-node
  echo
  echo "==> 完成：$NODE_BIN"
  echo "    下一步：./$(basename "$0") check"
}

# --- 启动前自检 -----------------------------------------------------------

do_check() {
  local errors=0

  echo "==> 1/6 协议版本预检（这一条会直接 core dump，而且跟操作系统无关）"
  # sui-node 在 AuthorityPerEpochStore::new 里对 protocol version 做硬 assert：
  # 网络 pv 高于二进制的 MAX_PROTOCOL_VERSION 时，进程 panic abort，报
  # "Please upgrade the binary"。该上限由基线 tag 决定，非 msim 编译下
  # MAX_ALLOWED == MAX，没有任何配置项或 CLI flag 能绕过 —— 唯一的解法是把补丁
  # 重打到更新的 mainnet tag。所以这一项放最前面：它比二进制、YAML 都更早致命。
  local pv_src="$REPO_ROOT/crates/sui-protocol-config/src/lib.rs"
  local pv_bin=""
  if [[ -f "$pv_src" ]]; then
    pv_bin="$(grep -m1 -oE 'const MAX_PROTOCOL_VERSION: u64 = [0-9]+' "$pv_src" | grep -oE '[0-9]+$' || true)"
  fi
  if [[ -z "$pv_bin" ]]; then
    warn "读不到本仓库的 MAX_PROTOCOL_VERSION（$pv_src 缺失或格式变了）"
  else
    ok "本仓库编出的 sui-node 最高支持 protocol version $pv_bin"
    # 主网当前 pv 不靠猜：优先扫上次启动的日志抓 panic 签名（完全离线、零误报），
    # 其次才是用户显式指定 PROTOCOL_RPC 时的主动比对。
    local pv_seen=""
    if [[ -f "$LOG_FILE" ]]; then
      pv_seen="$(grep -m1 -oE 'Network protocol version is ProtocolVersion\([0-9]+\)' "$LOG_FILE" 2>/dev/null | grep -oE '[0-9]+' || true)"
    fi
    if [[ -n "$pv_seen" && "$pv_seen" -gt "$pv_bin" ]]; then
      bad "上次启动就死在协议版本上：网络 pv $pv_seen > 本二进制上限 $pv_bin"
      echo "        依据：$LOG_FILE 里有 \"maximum supported version by the binary\" 的 panic。"
      echo "        这不是 Linux 的问题，也不是 MEV 补丁的问题：是基线版本落后于主网。"
      echo "        处理：把补丁重打到更新的 mainnet-vX.Y.Z tag，步骤见"
      echo "        docs/SUI_NODE_PATCH_IMPLEMENTATION.md 的『基线升级』一节。"
      echo "        不要手改常量放宽 assert —— 那只会让执行层与链上规则错位。"
      errors=$((errors + 1))
    elif [[ -n "$pv_seen" ]]; then
      ok "上次启动时网络 pv 为 $pv_seen ≤ 上限 $pv_bin（日志 $LOG_FILE）"
    elif [[ -n "$PROTOCOL_RPC" ]] && command -v curl >/dev/null 2>&1; then
      local pv_net=""
      pv_net="$(curl -fsS -m "$PROTOCOL_RPC_TIMEOUT" -H 'Content-Type: application/json' \
        -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"suix_getLatestSuiSystemState\",\"params\":[]}" \
        "$PROTOCOL_RPC" 2>/dev/null | grep -oE '"protocolVersion":"[0-9]+"' | grep -oE '[0-9]+' | head -1 || true)"
      if [[ -z "$pv_net" ]]; then
        warn "PROTOCOL_RPC=$PROTOCOL_RPC 没返回 protocolVersion（端点不支持 JSON-RPC 或未同步）"
      elif [[ "$pv_net" -gt "$pv_bin" ]]; then
        bad "网络已在 protocol version $pv_net，本二进制最高只支持 $pv_bin —— 启动必然 panic"
        echo "        处理：把补丁重打到更新的 mainnet-vX.Y.Z tag（见『基线升级』一节）。"
        errors=$((errors + 1))
      else
        ok "网络 protocol version $pv_net ≤ 二进制上限 $pv_bin"
      fi
    else
      ok "无 panic 记录，且未配 PROTOCOL_RPC（不联网）。先记住硬上限：$pv_bin"
      echo "        主网一旦升到 $((pv_bin + 1))，本二进制会在启动时直接 abort；"
      echo "        想提前比对：PROTOCOL_RPC=http://127.0.0.1:9000 ./$(basename "$0") check"
    fi
  fi

  echo "==> 2/6 二进制"
  if [[ -x "$NODE_BIN" ]]; then
    ok "$NODE_BIN"
  else
    bad "找不到可执行文件 $NODE_BIN"
    echo "        先编译：./$(basename "$0") build"
    errors=$((errors + 1))
  fi

  echo "==> 3/6 配置文件"
  if [[ -f "$CONFIG_PATH" ]]; then
    ok "$CONFIG_PATH"
    local genesis db_path
    genesis="$(yaml_nested_value 'genesis-file-location')"
    db_path="$(yaml_top_value 'db-path')"
    if [[ -n "$genesis" && -f "$genesis" ]]; then
      ok "genesis: $genesis"
    elif [[ -z "$genesis" ]]; then
      bad "YAML 里没找到 genesis-file-location"
      errors=$((errors + 1))
    else
      bad "genesis 文件不存在：$genesis"
      echo "        没有 genesis 节点无法启动。首次同步请改用官方快照。"
      errors=$((errors + 1))
    fi
    if [[ -n "$db_path" ]]; then
      if [[ -d "$db_path/live/store" ]]; then
        ok "DB store 已存在：$db_path/live/store"
      else
        warn "DB store 还不存在：$db_path/live/store"
        echo "        说明这台机器还没同步过。首次同步 mainnet 要跑很久，"
        echo "        强烈建议先下官方快照再启动，别硬同步。"
      fi
    fi
  else
    bad "配置文件不存在：$CONFIG_PATH"
    errors=$((errors + 1))
    db_path=""
  fi

  echo "==> 4/6 YAML 结构与 MEV socket 字段（这一步最容易出错）"

  # 结构检查优先：有重复键的话节点根本起不来，后面字段对不对都没意义
  local dups
  dups="$(duplicate_top_level_keys)"
  if [[ -n "$dups" ]]; then
    bad "YAML 里有重复的顶级键 —— serde_yaml 直接报错，节点 unwrap() panic 退出"
    local dk dc dl
    while IFS=$'\t' read -r dk dc dl; do
      echo "        ${dk} 出现 ${dc} 次（行 ${dl}）"
    done <<< "$dups"
    echo "        报错形如：duplicate field \`<键名>\`。同一个键的多条列表项要合并到"
    echo "        同一个键下面写成多个 - 项，不能把键名重复写两遍。"
    errors=$((errors + 1))
  else
    ok "无重复顶级键"
  fi

  local y_cache y_tx
  y_cache="$(yaml_top_value "$YKEY_CACHE")"
  y_tx="$(yaml_top_value "$YKEY_TX")"

  # 先抓 snake_case 误写：它会被 serde 静默忽略，不报错但功能没开
  if grep -qE '^mev_(cache_update|tx)_socket[[:space:]]*:' "$CONFIG_PATH" 2>/dev/null; then
    bad "检测到 snake_case 写法（mev_cache_update_socket / mev_tx_socket）"
    echo "        NodeConfig 是 #[serde(rename_all = \"kebab-case\")]，"
    echo "        snake_case 会被**静默忽略**，节点照常跑但推送功能没开。改成短横线。"
    errors=$((errors + 1))
  fi

  local pair
  for pair in "cache:$y_cache:$SOCKET_CACHE:$YKEY_CACHE" "tx:$y_tx:$SOCKET_TX:$YKEY_TX"; do
    local label="${pair%%:*}" rest="${pair#*:}"
    local actual="${rest%%:*}" want="${rest#*:}"; want="${want%:*}"
    local key="${pair##*:}"
    if [[ -z "$actual" ]]; then
      bad "$label 字段缺失：YAML 里没有 $key"
      errors=$((errors + 1))
    elif [[ "$actual" != "$want" ]]; then
      bad "$label 字段值与脚本期望不一致：YAML 是 '$actual'，脚本期望 '$want'"
      echo "        两边不一致时节点会 bind 到 YAML 那个路径，bot 连的是默认路径，"
      echo "        表现是'连不上但没有任何报错'。改其中一边对齐。"
      errors=$((errors + 1))
    else
      ok "$label -> $actual"
    fi
  done

  echo "==> 5/6 id 清单"
  if [[ -f "$POOL_IDS_PATH" ]]; then
    local lines bytes commas
    lines="$(wc -l < "$POOL_IDS_PATH" | tr -d ' ')"
    bytes="$(wc -c < "$POOL_IDS_PATH" | tr -d ' ')"
    commas="$(grep -c ',' "$POOL_IDS_PATH" 2>/dev/null || true)"
    ok "${POOL_IDS_PATH}（$lines 行 / $bytes 字节）"
    if [[ "${commas:-0}" -gt 0 ]]; then
      bad "文件里有 $commas 行含逗号 —— 这是老 relay-patch 的 id,related_id CSV 格式"
      echo "        我们的加载器会把它们全部当坏行 warn 掉，一条都加载不上。"
      echo "        正确格式：一行一个裸 ObjectID（0x + 64 hex）。"
      errors=$((errors + 1))
    fi
    if [[ "$RECORD_POOL_IDS" == "0" || "$RECORD_POOL_IDS" == "false" ]]; then
      warn "RECORD_POOL_IDS 被关掉了"
      echo "        注意：关掉后内存集合也**不再增长**（不只是不写文件），"
      echo "        新出现的池子永远不会被推送。除非明确知道自己在做什么，否则保持 1。"
    fi
    local parent_dir
    parent_dir="$(dirname "$POOL_IDS_PATH")"
    if [[ ! -w "$parent_dir" ]]; then
      bad "id 文件所在目录不可写：${parent_dir}（学到新 id 时无法落盘）"
      errors=$((errors + 1))
    fi
  else
    warn "id 清单还不存在：$POOL_IDS_PATH"
    echo "        正常：节点会空集启动，边跑边学，首次学到 id 时自动建父目录并创建文件。"
    echo "        但 bot 的 --preload-path 指向同一文件时，bot 侧会拿到空清单。"
  fi

  echo "==> 6/6 socket 现状"
  local s
  for s in "$SOCKET_CACHE" "$SOCKET_TX"; do
    if [[ -e "$s" ]]; then
      warn "$s 已存在（上次运行遗留，或已有节点在跑）"
      echo "        节点 bind 前会主动删掉遗留文件，不用手工处理。"
      echo "        但如果已有节点在跑，会把它踢掉 —— 先确认：./$(basename "$0") stop"
    else
      ok "$s 未占用"
    fi
  done

  echo
  if [[ $errors -gt 0 ]]; then
    echo "==> 自检发现 $errors 个问题，见上面 FAIL 项。修完再 start。"
    # 只有真的是 MEV 字段不对时，才附那段 YAML 模板
    if [[ "$(yaml_top_value "$YKEY_CACHE")" != "$SOCKET_CACHE" \
       || "$(yaml_top_value "$YKEY_TX")" != "$SOCKET_TX" ]]; then
      echo
      echo "需要在 $CONFIG_PATH 顶层（与其他顶级字段平级，不要嵌进 p2p-config/rpc）补上："
      echo
      echo "    $YKEY_CACHE: \"$SOCKET_CACHE\""
      echo "    $YKEY_TX:           \"$SOCKET_TX\""
    fi
    echo
    return 1
  fi
  echo "==> 自检全部通过。"
  return 0
}

# --- 启动 -----------------------------------------------------------------

apply_env() {
  # 这三个变量都是每进程读一次（OnceLock），改了必须重启，热改无效。
  export SUI_RECORD_POOL_RELATED_IDS="$RECORD_POOL_IDS"
  export SUI_POOL_RELATED_IDS_PATH="$POOL_IDS_PATH"
  if [[ -n "$WATCHED_OWNERS" ]]; then
    export SUI_MEV_WATCHED_OWNERS="$WATCHED_OWNERS"
  else
    unset SUI_MEV_WATCHED_OWNERS 2>/dev/null || true
  fi

  echo "==> 环境变量"
  echo "    SUI_RECORD_POOL_RELATED_IDS=$SUI_RECORD_POOL_RELATED_IDS"
  echo "    SUI_POOL_RELATED_IDS_PATH=$SUI_POOL_RELATED_IDS_PATH"
  if [[ -n "$WATCHED_OWNERS" ]]; then
    echo "    SUI_MEV_WATCHED_OWNERS=$WATCHED_OWNERS"
  else
    echo "    SUI_MEV_WATCHED_OWNERS=<未设置>（只按 id 清单过滤）"
  fi
  echo "==> 注意：编译产物只是二进制，启动靠下面这条命令 + 上面的 env + YAML 里的字段"
  echo
}

do_start() {
  do_check || die "自检未通过。确认要跳过检查的话，直接跑：$NODE_BIN --config-path $CONFIG_PATH"
  apply_env
  echo "==> 前台启动（Ctrl-C 停止）"
  echo "    验证请另开一个终端：./$(basename "$0") verify"
  echo
  exec "$NODE_BIN" --config-path "$CONFIG_PATH"
}

do_daemon() {
  do_check || die "自检未通过。"
  apply_env
  : > "$LOG_FILE"
  echo "==> 后台启动，日志 -> $LOG_FILE"
  nohup "$NODE_BIN" --config-path "$CONFIG_PATH" >"$LOG_FILE" 2>&1 &
  local pid=$!
  echo "$pid" > "$PID_FILE"
  echo "    pid=${pid}（写在 ${PID_FILE}）"
  echo
  echo "    节点初始化要几秒到几十秒，等一会儿再验证："
  echo "      sleep 20 && ./$(basename "$0") verify"
  echo "    看实时日志：tail -f $LOG_FILE"
  echo "    停止：      ./$(basename "$0") stop"
}

do_stop() {
  if [[ ! -f "$PID_FILE" ]]; then
    echo "没有 ${PID_FILE}，daemon 模式没在跑（前台 start 请用 Ctrl-C 停止）。"
    return 0
  fi
  local pid
  pid="$(cat "$PID_FILE")"
  if kill -0 "$pid" 2>/dev/null; then
    echo "==> 优雅停止 pid=${pid}（SIGTERM，让节点把 DB 收尾写干净）"
    kill "$pid"
    local i=0
    while kill -0 "$pid" 2>/dev/null && [[ $i -lt 60 ]]; do
      sleep 1
      i=$((i + 1))
    done
    if kill -0 "$pid" 2>/dev/null; then
      warn "60 秒还没退出，节点可能卡在收尾。确认后再强杀：kill -9 $pid"
      warn "强杀有 DB 损坏风险，下次启动可能需要重下快照。"
    else
      ok "已退出"
    fi
  else
    warn "pid=$pid 已经不在了"
  fi
  rm -f "$PID_FILE"
}

# --- 启动后验证 -----------------------------------------------------------

do_verify() {
  echo "==> 验证补丁是否真的生效"
  echo "    （只能证明节点侧 bind 成功、清单在加载；端到端链路仍需 bot 侧确认）"
  echo

  local alive=0
  if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    ok "节点进程在跑：pid=$(cat "$PID_FILE")"
    alive=1
  elif pgrep -f "sui-node --config-path" >/dev/null 2>&1; then
    warn "有 sui-node 在跑，但不是本脚本 daemon 模式起的（没有 pid 文件）"
    alive=1
  else
    bad "没看到 sui-node 进程"
    echo "        先启动：./$(basename "$0") daemon"
  fi

  # 1) socket 文件存在 = YAML 字段被读到了 + bind 成功。
  #    这是抓"字段名拼错被静默忽略"的唯一硬证据。
  #    但进程不在时文件可能只是上次崩溃的残留，不能算生效，所以分两种报法。
  local s
  for s in "$SOCKET_CACHE" "$SOCKET_TX"; do
    if [[ -S "$s" && $alive -eq 1 ]]; then
      ok "socket 已 bind：$s"
    elif [[ -S "$s" ]]; then
      warn "$s 在，但节点进程不在 —— 这是上次崩溃/未清理的残留 socket"
      echo "        不算生效证据。真启动时节点会先删掉它再 bind（mev_socket.rs 的 bind_socket）。"
    elif [[ -e "$s" ]]; then
      bad "$s 存在但不是 socket 文件"
    else
      bad "$s 不存在"
      echo "        两种可能：(a) YAML 字段名拼错/snake_case，被静默忽略了；"
      echo "                  (b) bind 失败。查日志：grep -i 'failed to bind' $LOG_FILE"
    fi
  done

  # 2) 日志侧证据
  if [[ -f "$LOG_FILE" ]]; then
    echo
    echo "  -- 日志关键行（${LOG_FILE}）--"
    local hits
    hits="$(grep -iE 'pool_related|failed to bind|jsonrpc index|creating state' "$LOG_FILE" 2>/dev/null | tail -12 || true)"
    if [[ -n "$hits" ]]; then
      printf '     %s\n' "$hits"
    else
      warn "日志里还没有 pool_related 相关行"
      echo "        id 清单是**首次有 cache miss 时**才懒加载的，刚启动可能还没触发。"
      echo "        等节点同步出几个 checkpoint 再看。"
    fi
  else
    warn "没有 ${LOG_FILE}（前台 start 模式日志在终端里，不在文件）"
  fi

  # 3) id 清单是否在增长
  if [[ -f "$POOL_IDS_PATH" ]]; then
    echo
    local now
    now="$(wc -l < "$POOL_IDS_PATH" | tr -d ' ')"
    ok "id 清单当前 $now 行"
    echo "        再跑几分钟看一次，行数应随新池子出现而增长："
    echo "        ./$(basename "$0") verify   # 或 wc -l $POOL_IDS_PATH"
  fi

  echo
  echo "==> bot 侧对接命令（先起节点、再起 bot；socket 用默认值即可，无需传）"
  local db_path store
  db_path="$(yaml_top_value 'db-path')"
  store="${db_path%/}/live/store"
  cat <<EOF
    cd <sui-mev 仓库>
    cargo run --release --bin arb -- start-bot \\
        --private-key <你的 base64 密钥对> \\
        --rpc-url http://localhost:9000 \\
        --db-path "$store" \\
        --config-path "$CONFIG_PATH" \\
        --preload-path "$POOL_IDS_PATH"
EOF
  echo
  echo "    说明："
  echo "      --rpc-url      RPC 是必需的（提交交易、找 gas coin、gas price、"
  echo "                     get_dynamic_fields 都走它），补丁只把试算搬到了本地 DB。"
  echo "                     指向本机节点自己，别用第三方 fullnode —— gas coin 从 RPC 读、"
  echo "                     池子状态从本地 DB 读，两个源不同步会导致交易直接失败。"
  echo "      --db-path      必须等于 <YAML db-path>/live/store，否则 simulator 打不开 DB。"
  echo "      --config-path  同一份 fullnode.yaml；bot 会读它拿 genesis，对不上就报错。"
  echo "      --preload-path 与节点共用一份 id 清单，两端不一致是最常见的'没推送'原因。"
  echo "      本地 DB 试算已是唯一路径：bot 侧的 --use-db-simulator 开关与 HttpSimulator 一起"
  echo "      删掉了（sui-mev 提交 acb9a9f），传这个 flag 会被 clap 直接拒绝。"
}

usage() {
  cat <<'USAGE'
用法：./start_mev_node.sh <子命令>

  build    cargo build --release --bin sui-node（工具链由 rust-toolchain.toml 钉住）
  check    启动前自检（二进制/配置/YAML 字段/id 清单/socket），不启动任何东西
  start    前台启动（Ctrl-C 停），日志直接到终端
  daemon   后台启动，日志 -> /tmp/sui-node-mev.log，pid -> /tmp/sui-node-mev.pid
  verify   启动后验证：进程在不在、两个 socket 有没有真 bind、清单行数、日志关键行、
           并打印 bot 侧可直接执行的对接命令
  stop     对 daemon 模式起的节点发 SIGTERM（优雅关，让节点把 DB 收尾写干净）

典型顺序：build -> check -> daemon -> (等 20s) -> verify

可用环境变量覆盖的变量（完整说明看文件头注释）：
  NODE_BIN CONFIG_PATH POOL_IDS_PATH RECORD_POOL_IDS WATCHED_OWNERS
  SOCKET_CACHE SOCKET_TX LOG_FILE PID_FILE

例：只本次启动不学习新 id（调试用，见文件头里“不能长期关”的原因）：
  RECORD_POOL_IDS=0 ./start_mev_node.sh daemon
USAGE
}

# --- 入口 -----------------------------------------------------------------

case "${1:-}" in
  build)  do_build ;;
  check)  do_check ;;
  start)  do_start ;;
  daemon) do_daemon ;;
  stop)   do_stop ;;
  verify) do_verify ;;
  --help|-h|help) usage ;;
  "")     die "缺子命令。用法：$0 build | check | start | daemon | verify | stop（或 $0 help）" ;;
  *)      die "未知子命令：$1（可用：build check start daemon verify stop help）" ;;
esac
