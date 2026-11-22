#!/usr/bin/env python3
"""单向限速 TCP 代理：被代理方→上游不限速，上游→被代理方按 B/s 限速。

`benches/replica_redo.sh` 用：localhost 上快照下载比主侧一次 checkpoint 的编码还
快（164 MB 状态：下载 ~1.5 s，编码 ~4-5 s），换代永远落在下载结束之后，测不到
「前缀复用」。把下行压到 20 MB/s，换代就必然落在下载窗口的中段。

用法：throttle.py <listen_port> <upstream_port> <down_bytes_per_sec>
"""
import socket
import sys
import threading
import time

listen_port = int(sys.argv[1])
up_addr = ("127.0.0.1", int(sys.argv[2]))
rate = float(sys.argv[3])  # 下行字节/秒，0 = 不限


def pipe(src, dst, limit):
    budget = 0.0
    last = time.time()
    try:
        while True:
            b = src.recv(65536)
            if not b:
                break
            if limit > 0:
                now = time.time()
                budget += (now - last) * limit
                last = now
                if budget > limit:  # 不许攒太多，否则一个突发就绕过限制
                    budget = limit
                while budget < len(b):
                    time.sleep(min((len(b) - budget) / limit, 0.5))
                    now = time.time()
                    budget += (now - last) * limit
                    last = now
                budget -= len(b)
            dst.sendall(b)
    except OSError:
        pass
    finally:
        try:
            dst.shutdown(socket.SHUT_WR)
        except OSError:
            pass


def serve(c):
    try:
        u = socket.create_connection(up_addr)
    except OSError:
        c.close()
        return
    t = threading.Thread(target=pipe, args=(c, u, 0), daemon=True)
    t.start()
    pipe(u, c, rate)
    t.join(timeout=5)
    c.close()
    u.close()


srv = socket.socket()
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", listen_port))
srv.listen(64)
print(f"throttle {listen_port} -> {up_addr} at {rate} B/s down", flush=True)
while True:
    conn, _ = srv.accept()
    threading.Thread(target=serve, args=(conn,), daemon=True).start()
