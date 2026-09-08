#!/usr/bin/env python3
"""Verify the two MEV push sockets a patched sui-node writes to.

The node is the listener; this script is the subscriber. That distinction matters:
`SocketFanOut::push` short-circuits unless `has_subscribers()` is true, so no client ==
no bytes. Connecting *is* what turns the push path on.

Wire formats, read off the node source:

  cache socket   crates/sui-core/src/cache_update_handler.rs
      [ u32 LE length ][ BCS Vec<(ObjectID, Object)> ]
      matches the bot reader in crates/simulator/src/db_simulator/mod.rs (from_le_bytes)

  tx socket      crates/sui-core/src/tx_handler.rs
      [ u32 BE length ][ bincode TransactionEffects ]
      [ u32 BE length ][ serde_json Vec<SuiEvent>   ]
      matches the bot reader in bin/arb/src/collector.rs (from_be_bytes)

Different byte order on the two streams is intentional, not a typo.

What counts as proof here, in order of strength:

1. Stream alignment. Length prefixes are the only sync in this protocol. Decode N
   frames back to back and every frame is evidence about the previous one: get a byte
   order or a length wrong and the *next* prefix comes out absurd. So "N frames, no
   misalignment" proves framing, which is what breaks silently in production.
2. Decoded *values*, not just framing. The cache stream's object ids must be members of
   the node's own pool id list (--pool-ids); the tx stream's event JSON must carry a
   base58 digest that decodes to 32 bytes. Both are checked without trusting RPC.
3. Cross-checking a decoded id/digest against an external reader. Only possible once the
   node has synced, so it is left to --explain at the end.

Usage:
    ./mev_socket_probe.py cache
    ./mev_socket_probe.py tx --count 5 --show-first
    ./mev_socket_probe.py cache --pool-ids /Volumes/superfs/suidata/db/pool_related_ids.txt
"""

import argparse
import base64
import json
import os
import socket
import struct
import sys
import time
from collections import Counter

DEFAULT_SOCKETS = {
    "cache": "/tmp/sui_cache_updates.sock",
    "tx": "/tmp/sui_tx.sock",
}

DEFAULT_POOL_IDS = "/Volumes/superfs/suidata/db/pool_related_ids.txt"

# A frame this big means we are misaligned rather than looking at real traffic: one
# cache frame is the pool objects written by a single transaction.
MAX_FRAME = 32 * 1024 * 1024


class FrameError(Exception):
    """A frame could not be decoded, which means the stream is misaligned or the
    assumed format does not match what the node actually wrote."""


# ---------------------------------------------------------------- framing primitives


def read_exact(sock, n):
    if n > MAX_FRAME:
        raise FrameError(f"长度前缀声称 {n} 字节，超过 {MAX_FRAME} 上限 —— 流已错位")
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            if not buf:
                return None  # clean EOF between frames
            raise FrameError(f"半路断流：需要 {n} 字节，只读到 {len(buf)}")
        buf.extend(chunk)
    return bytes(buf)


def decode_uleb128(payload, offset):
    """BCS sequence lengths are ULEB128."""
    value, shift = 0, 0
    while offset < len(payload):
        byte = payload[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, offset
        shift += 7
        if shift > 63:
            raise FrameError("ULEB128 长度编码超长")
    raise FrameError("payload 在一个 ULEB128 长度中间结束")


# ---------------------------------------------------------------------- content checks


def load_pool_ids(path):
    """The node's self-learned pool object ids, one hex id per line."""
    if not os.path.exists(path):
        return None
    ids = set()
    with open(path) as handle:
        for line in handle:
            line = line.strip()
            if line:
                ids.add(int(line, 16) if not line.isdigit() else int(line))
    return ids


BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def b58decode(value):
    """Sui 的交易摘要是 base58（Bitcoin 字母表）。Python 标准库没有 base58，
       base64 也没有 b58decode，所以自己算。只用于校验能不能解出 32 字节。"""
    num = 0
    for ch in value:
        digit = BASE58_ALPHABET.find(ch)
        if digit < 0:
            raise ValueError(f"非 base58 字符: {ch!r}")
        num = num * 58 + digit
    raw = num.to_bytes((num.bit_length() + 7) // 8, "big") if num else b""
    # 前导 '1' 是编码进去的 0 字节
    pad = len(value) - len(value.lstrip("1"))
    return b"\x00" * pad + raw


def check_tx_digest(value):
    """SuiTransactionBlockDigest serialises to base58 in JSON. 32 bytes when decoded."""
    if not isinstance(value, str) or not value:
        return f"缺失或非字符串: {value!r}"
    try:
        raw = b58decode(value)
    except Exception as exc:
        return f"base58 解不开: {exc}"
    if len(raw) != 32:
        return f"解出 {len(raw)} 字节，应为 32"
    return None


def probe_cache_frame(payload, ids_seen, want_total):
    """Structural decode of BCS Vec<(ObjectID, Object)>.

    ObjectID is 32 raw bytes under BCS. `Object` is variable-length and needs the real
    Rust types to walk, so we read the leading ids (enough for membership checking) and
    assert the payload is at least large enough to hold what the vector claims.
    """
    count, offset = decode_uleb128(payload, 0)
    if count == 0:
        raise FrameError("对象数为 0：notify_written 明确跳过空 vec，不该出现在线上")
    # An object body is never shorter than a few bytes, so this is a cheap consistency
    # test between the claimed count and the actual frame size.
    if len(payload) < offset + count * 32:
        raise FrameError(f"声称 {count} 个对象，但 payload 只有 {len(payload)} 字节")

    hexed = []
    cursor = offset
    for _ in range(min(count, want_total)):
        obj_id = payload[cursor:cursor + 32]
        cursor += 32
        if not any(obj_id):
            raise FrameError("解出一个全零 ObjectID —— 偏移不对")
        hexed.append("0x" + obj_id.hex())
    ids_seen.update(hexed)
    return count, hexed


def probe_tx_events(events_raw):
    """The events half is JSON, so this stream can be checked down to field level.

    SuiEvent's shape comes from #[serde(rename_all = "camelCase")] in
    crates/sui-json-rpc-types/src/sui_event.rs: the digest lives inside `id` as
    `txDigest`, and `type_` loses its trailing underscore.
    """
    try:
        events = json.loads(events_raw.decode("utf-8"))
    except Exception as exc:
        raise FrameError(f"events 半帧不是合法 JSON: {exc}")
    if not isinstance(events, list) or not events:
        raise FrameError("events 不是非空数组：节点侧对空事件直接 return，说明读歪了")
    for event in events:
        if not isinstance(event, dict):
            raise FrameError(f"events 元素不是对象: {type(event).__name__}")
        for field in ("id", "packageId", "sender", "parsedJson"):
            if field not in event:
                raise FrameError(f"events 元素缺字段 {field}（实际字段: {sorted(event)[:8]}）")
        problem = check_tx_digest((event.get("id") or {}).get("txDigest"))
        if problem:
            raise FrameError(f"id.txDigest {problem}")
    return events


# ------------------------------------------------------------------------- the streams


def run_cache(sock, args, pool_ids):
    frames = 0
    bytes_read = 0
    ids_seen = set()
    missing_from_pool = set()
    while frames < args.count:
        head = read_exact(sock, 4)
        if head is None:
            break
        length = struct.unpack("<I", head)[0]  # little endian
        if length < 5:
            raise FrameError(f"payload 长度 {length} 装不下任何对象")
        payload = read_exact(sock, length)
        if payload is None:
            raise FrameError(f"前缀说 {length} 字节，流却提前结束")
        frames += 1
        bytes_read += 4 + length
        count, hexed = probe_cache_frame(payload, ids_seen, args.dump_ids)

        if pool_ids is not None:
            for one in hexed:
                if int(one, 16) not in pool_ids:
                    missing_from_pool.add(one)

        if not args.quiet:
            print(f"  cache #{frames:<5} 对象数={count:<4} 帧长={length:<7} "
                  f"首个={hexed[0]}")
            for extra in hexed[1:]:
                print(f"          id={extra}")

    if pool_ids is not None and missing_from_pool:
        print(f"\n  注意：{len(missing_from_pool)} 个 id 不在 pool 清单里。")
        print("  这未必是 bug：SUI_MEV_WATCHED_OWNERS 命中地址的交易也会推。")
        for one in sorted(missing_from_pool)[:5]:
            print(f"    {one}")
    return frames, bytes_read, {
        "distinctObjectIds": len(ids_seen),
        "notInPoolList": len(missing_from_pool),
        "sampleIds": sorted(ids_seen)[:5],
    }


def run_tx(sock, args, _pool_ids):
    frames = 0
    bytes_read = 0
    type_tally = Counter()
    digests = []
    while frames < args.count:
        head = read_exact(sock, 4)
        if head is None:
            break
        effects_len = struct.unpack(">I", head)[0]  # big endian
        if effects_len == 0:
            raise FrameError("effects 长度为 0")
        effects = read_exact(sock, effects_len)
        if effects is None:
            raise FrameError("effects 半帧被截断")

        head = read_exact(sock, 4)
        if head is None:
            raise FrameError("读完 effects 就断，没等到 events 前缀")
        events_len = struct.unpack(">I", head)[0]  # big endian
        if events_len < 2:
            raise FrameError(f"events 长度 {events_len} 装不下一个 JSON 数组")
        events_raw = read_exact(sock, events_len)
        if events_raw is None:
            raise FrameError("events 半帧被截断")

        frames += 1
        bytes_read += 8 + effects_len + events_len
        events = probe_tx_events(events_raw)
        type_tally.update(e.get("type", "?") for e in events)
        digests.append((events[0].get("id") or {}).get("txDigest"))

        if not args.quiet:
            print(f"  tx #{frames:<5} 事件数={len(events):<3} effects={effects_len:<6}B "
                  f"events={events_len:<6}B "
                  f"digest={str(digests[-1])[:12]}.. "
                  f"type={events[0].get('type', '?')}")

        if args.show_first and frames == 1:
            print(json.dumps(events, indent=2, ensure_ascii=False)[:3000])

    return frames, bytes_read, {
        "distinctTx": len(set(digests)),
        "topEventTypes": type_tally.most_common(5),
        "sampleDigests": digests[:3],
    }


STREAMS = {"cache": run_cache, "tx": run_tx}


def main():
    ap = argparse.ArgumentParser(
        description="Decode and sanity-check the node's MEV push sockets.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("stream", choices=["cache", "tx"])
    ap.add_argument("--socket", help="覆盖默认 socket 路径")
    ap.add_argument("--count", type=int, default=30, help="解析多少帧后收工（默认 30）")
    ap.add_argument("--timeout", type=float, default=60.0, help="总时长上限秒数（默认 60）")
    ap.add_argument("--quiet", action="store_true", help="只打汇总")
    ap.add_argument("--json", action="store_true", help="汇总用 JSON 打")
    ap.add_argument("--pool-ids", default=DEFAULT_POOL_IDS,
                    help=f"cache 流用它核对对象归属（默认 {DEFAULT_POOL_IDS}）")
    ap.add_argument("--strict-pool", action="store_true",
                    help="有对象 id 不在 pool 清单里时退出码改为 4（默认只提示不报错）")
    ap.add_argument("--dump-ids", type=int, default=4, help="每帧最多打几个对象 id（默认 4）")
    ap.add_argument("--show-first", action="store_true", help="打第一条 tx 流的完整事件 JSON")
    args = ap.parse_args()

    path = args.socket or DEFAULT_SOCKETS[args.stream]
    if not os.path.exists(path):
        print(f"FAIL  {path} 不存在。\n"
              f"      节点在跑吗？字段名写成 snake_case 会被 serde 静默忽略。", file=sys.stderr)
        return 2

    pool_ids = load_pool_ids(args.pool_ids) if args.stream == "cache" else None
    if args.stream == "cache":
        if pool_ids is None:
            print(f"提示：{args.pool_ids} 不存在，跳过对象归属核对（用 --pool-ids 指定）")
        else:
            print(f"提示：pool 清单 {len(pool_ids)} 个 id，将用于核对对象归属")

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.connect(path)
    except OSError as exc:
        print(f"FAIL  connect {path} 失败：{exc}", file=sys.stderr)
        return 2
    print(f"==> 已作为 subscriber 连上 {path}（推送路径由此激活）")

    started = time.monotonic()
    error = None
    detail = {}
    frames = bytes_read = 0
    try:
        # A blocking socket plus an overall alarm: without this the read just waits
        # forever when there is no traffic, which looks identical to a broken patch.
        sock.settimeout(max(1.0, args.timeout - (time.monotonic() - started)))
        frames, bytes_read, detail = STREAMS[args.stream](sock, args, pool_ids)
    except socket.timeout:
        error = "等待超时：连接活着，但节点在这期间一帧都没推"
    except FrameError as exc:
        error = f"帧解析失败：{exc}"
    except Exception as exc:  # keep the diagnosis honest rather than a bare traceback
        error = f"{type(exc).__name__}: {exc}"
    finally:
        sock.close()

    elapsed = time.monotonic() - started
    summary = {"stream": args.stream, "frames": frames, "bytes": bytes_read,
               "seconds": round(elapsed, 1)}
    summary.update(detail)
    print()
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        print(f"==> 结果：{frames} 帧 / {bytes_read} 字节 / {elapsed:.1f} 秒")
        for key, value in detail.items():
            print(f"    {key}: {value}")

    if error:
        print(f"\n  !! {error}")
        if frames == 0:
            print_explanation(args.stream)
        return 1
    if frames == 0:
        print_explanation(args.stream)
        return 1
    # 默认不当错误：SUI_MEV_WATCHED_OWNERS 命中的地址本来就会推出清单外的对象。
    # 要“只允许清单内对象”的硬约束时显式加 --strict-pool。
    if args.strict_pool and detail.get("notInPoolList"):
        print(f"\n  --strict-pool：{detail['notInPoolList']} 个 id 不在 pool 清单里。")
        print("  要么是没开 watched owner 却收到了清单外对象（推送过滤有问题），")
        print("  要么节点和 bot 用的不是同一份清单（最常见的‘没推送/推错’原因）。")
        return 4
    if not args.json:
        print(f"\n    连续 {frames} 帧长度前缀自洽 -> 帧格式与字节序正确")
    return 0


def print_explanation(stream):
    print("\n    一帧都没收到。这不是格式错误，按概率排：")
    print("      1) 节点还没同步出已执行的 checkpoint。cache/tx 两条流都只在提交路径上")
    print("         触发，创世之后没有任何交易被执行时它们本来就该是空的。查：")
    print("         curl -s -X POST http://127.0.0.1:9000 -H 'Content-Type: application/json' \\")
    print("           -d '{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"sui_getLatestCheckpointSequenceNumber\",\"params\":[]}'")
    print("         返回值还是 \"0\" 就说明没进展，跟补丁无关。")
    if stream == "cache":
        print("      2) cache 流只推命中 pool 清单或 watched owner 的对象")
        print("         （authority.rs notify_mev_cache_updates）。清单为空 -> 永远不推。")
    else:
        print("      2) tx 流跳过没有事件的交易（tx_handler.rs notify_effects_events）")
        print("         以及 written 为空的交易（authority.rs 推送点）。")
    print("      3) 系统交易两条流都跳过，所以只有系统交易在跑时也不会有输出。")
    print("      4) 节点日志应有 \"listening for subscribers\" 和 \"subscriber connected\"；")
    print("         后者没有 = 连的不是这个 listener。")


if __name__ == "__main__":
    sys.exit(main())
