#!/usr/bin/env python3
"""Self-test for mev_socket_probe.py: feed it synthetic frames with known content.

Why this exists: the node is a fullnode sitting at checkpoint 0, so both push sockets
are legitimately silent right now. A probe that prints "0 frames" proves nothing about
whether the probe itself is correct. So here we play the role of the node over a real
unix socket and assert two things:

  * frames written in the documented format decode cleanly (positive cases), and
  * frames written *wrongly* -- swapped length byte order, a truncated frame, a stray
    byte, non-JSON events -- are REJECTED rather than silently accepted (negative cases).

Without the negative cases the tool could pass by parsing nothing or by mis-parsing
everything, which is exactly the failure mode that would waste a node sync.

Socket paths must stay under macOS' 104-byte AF_UNIX limit, hence the short tmpdir.
"""

import json
import os
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time

REPO = os.path.dirname(os.path.abspath(__file__))
PROBE = os.path.join(REPO, "mev_socket_probe.py")

POOL_ID = "6" * 64      # pretends to be a learned pool object
FOREIGN_ID = "f" * 64   # not in the list
B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def b58encode(raw):
    num = int.from_bytes(raw, "big")
    out = ""
    while num:
        num, rem = divmod(num, 58)
        out = B58[rem] + out
    pad = len(raw) - len(raw.lstrip(b"\x00"))
    return "1" * pad + out


# A digest that really decodes to 32 bytes, so the probe's base58 check is exercised
# by a valid value rather than accidentally passing on garbage.
TX_DIGEST = b58encode(bytes([0x0A]) + bytes(range(1, 32)))


def uleb(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def object_body(version):
    """Variable-length stand-in for one BCS-encoded Object."""
    return bytes([0x01, 0x20]) + bytes([version]) * 30


def object_body_padded(seed):
    """An Object body the way mainnet actually writes it: 33 leading zero bytes.

    ObjectData variant (00) + TypeTag variant (00) + a type address such as
    0x2::coin::Coin (31 leading zeros) = 33 consecutive zeros before anything nonzero.
    Every other body here is nonzero filler, which is exactly why a decoder that walks
    elements on a fixed 32-byte stride could pass this selftest and still fall over on
    live traffic: the window at the start of element #1's body is all zeros.
    """
    return b"\x00" * 33 + b"\x02" + bytes([seed]) * 24


def cache_frame(ids, body=object_body):
    payload = uleb(len(ids))
    for i, oid in enumerate(ids):
        payload += bytes.fromhex(oid) + body(i + 1)
    return struct.pack("<I", len(payload)) + payload   # cache length is LE


def tx_frame(n_events=2, digest=TX_DIGEST):
    events = []
    for i in range(n_events):
        events.append({
            "id": {"txDigest": digest, "eventSeq": str(i)},
            "packageId": "0x" + POOL_ID,
            "transactionModule": "pool",
            "sender": "0x" + "1" * 64,
            "type": "0x5494::pool::Swap",
            "parsedJson": {"amount": str(1000 + i), "recipient": "0x" + "2" * 64},
            "bcsEncoding": "base64",
            "bcs": "AAECAw==",
            "timestampMs": "1700000000000",
        })
    ev = json.dumps(events).encode()
    effects = bytes([0x05]) * 37  # opaque bincode stand-in
    # tx lengths are BE, deliberately the opposite of the cache stream
    return struct.pack(">I", len(effects)) + effects + struct.pack(">I", len(ev)) + ev


def serve(path, frames, hold=0.6):
    """Act as the node: accept one subscriber, push frames, then hang up."""
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(path)
    srv.listen(1)

    def loop():
        try:
            conn, _ = srv.accept()
            for frame in frames:
                conn.sendall(frame)
                time.sleep(0.02)
            time.sleep(hold)  # let the reader finish before hanging up
            conn.close()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass  # the probe exits as soon as its count is met; that's expected
        finally:
            try:
                srv.close()
                os.unlink(path)
            except OSError:
                pass

    threading.Thread(target=loop, daemon=True).start()


def run_probe(stream, path, extra=()):
    cmd = [sys.executable, PROBE, stream, "--socket", path,
           "--timeout", "6", "--pool-ids", IDLIST] + list(extra)
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    return proc.returncode, proc.stdout + proc.stderr


results = []
TMP = tempfile.mkdtemp(prefix="mp")  # short: AF_UNIX caps the path at 104 bytes
IDLIST = os.path.join(TMP, "ids.txt")
with open(IDLIST, "w") as fh:
    fh.write("0x" + POOL_ID + "\n")


def case(name, want_rc, stream, frames, extra=()):
    path = os.path.join(TMP, f"{len(results)}.sock")
    serve(path, frames)
    rc, out = run_probe(stream, path, extra)
    ok = rc == want_rc
    results.append((ok, name, rc, want_rc, out))
    print(f"{'PASS' if ok else 'FAIL'}  {name:<26} rc={rc} (期望 {want_rc})")
    if not ok:
        body = [ln for ln in out.splitlines()
                if any(k in ln for k in ("!!", "FAIL", "结果", "不在", "解析失败", "注意"))]
        for line in body[:8]:
            print(f"        {line.strip()}")


print("==> 正向：文档格式必须解得开")
case("cache_2frames", 0, "cache",
     [cache_frame([POOL_ID, FOREIGN_ID]), cache_frame([POOL_ID])])
case("tx_2frames", 0, "tx", [tx_frame(2), tx_frame(1)])
case("cache_many_frames", 0, "cache", [cache_frame([POOL_ID]) for _ in range(12)],
     ["--count", "12"])

print("\n==> 语义：对象归属核对（节点只推清单内对象或 watched owner）")
case("pool_id_in_list", 0, "cache", [cache_frame([POOL_ID])])
case("foreign_needs_strict", 0, "cache", [cache_frame([FOREIGN_ID])])
case("foreign_strict_exit4", 4, "cache", [cache_frame([FOREIGN_ID])], ["--strict-pool"])

# The two cases that were missing. A multi-object frame with realistic zero-padded
# bodies is ordinary mainnet traffic, and it is what the fixed-stride walk read back as
# "all-zero ObjectID" on a perfectly healthy node.
case("multi_real_bodies", 0, "cache",
     [cache_frame([POOL_ID, POOL_ID], body=object_body_padded)])
case("multi_real_bodies_strict", 0, "cache",
     [cache_frame([POOL_ID, FOREIGN_ID], body=object_body_padded)], ["--strict-pool"])

print("\n==> 负向：错格式必须被拒（这是探针有没有用的关键）")
good = cache_frame([POOL_ID])
payload = good[4:]
case("cache_len_be_rejected", 1, "cache", [struct.pack(">I", len(payload)) + payload])

tx = tx_frame(1)
eff, ev = tx[4:4 + 37], tx[4 + 37 + 4:]
case("tx_len_le_rejected", 1, "tx",
     [struct.pack("<I", 37) + eff + struct.pack("<I", len(ev)) + ev])
case("cache_truncated", 1, "cache", [good[:len(good) - 9]])
case("cache_stray_junk", 1, "cache", [good, b"\xff\xff\xff\xff" + b"\x00" * 20])
case("tx_events_not_json", 1, "tx",
     [struct.pack(">I", 5) + b"\x01\x02\x03\x04\x05" + struct.pack(">I", 6) + b"not-a-xx"])
case("tx_bad_base58_digest", 1, "tx", [tx_frame(1, digest="0OIl" + "x" * 40)])

print("\n==> 诚实性：没有流量时报 0，不能装作通过")
case("empty_stream", 1, "cache", [])

print()
passed = sum(1 for r in results if r[0])
print(f"==> 自测 {passed}/{len(results)} 通过")
if passed != len(results):
    print("    探针不可信，先修 mev_socket_probe.py 再谈节点验证。")
sys.exit(0 if passed == len(results) else 1)
