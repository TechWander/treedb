/* 单核 loopback TCP 往返下界 —— 不碰 treedb 任何代码路径。
 *
 * 为什么要它：要回答「ping 在 1 核上跑到 15 万 ops/s 是我们的代码慢，还是这台
 * 机器的内核就这么多」，光有 utime/stime 拆分还不够（stime 里也可能是我们自己
 * 触发的工作）。这个程序把服务端做成最小形态：epoll 等若干已建立的连接，收到
 * 多少字节就原样发回多少，除此之外一个字都不算。它跑出的 ops/s 就是「同核、同
 * 连接数、同 pipeline=1 口径下，任何实现都不可能超过的那条线」。
 *
 * 两个口径上的讲究（都是踩过坑之后定的）：
 *   1. 服务端单独一条线程钉死在被测核；客户端每个连接一条线程、**不钉核**，
 *      否则单线程轮询 64 连接会把客户端的串行往返算进分母，量出来的是客户端。
 *   2. 每客户端线程自己累加 ops 到独立槽位，收尾时汇总 —— 不用共享计数器，
 *      避免把原子争用当成被测成本。
 *
 * 用法：lb_floor <server_cpu> <conns> <secs>
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sched.h>
#include <pthread.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <time.h>

static int srv_fd;
static int g_secs = 3;
static volatile int g_stop = 0;

static double now_s(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec + t.tv_nsec / 1e9;
}

/* ---- 服务端：epoll_wait + recv + send，无业务、无分配 ---- */
static void *server_thread(void *arg) {
    (void)arg;
    int ep = epoll_create1(0);
    struct epoll_event ev = {.events = EPOLLIN, .data.fd = srv_fd};
    epoll_ctl(ep, EPOLL_CTL_ADD, srv_fd, &ev);
    struct epoll_event out[128];
    char *buf = malloc(65536);
    for (;;) {
        int n = epoll_wait(ep, out, 128, 50);
        for (int i = 0; i < n; i++) {
            int fd = out[i].data.fd;
            if (fd == srv_fd) {
                int c = accept(srv_fd, NULL, NULL);
                if (c < 0) continue;
                int one = 1;
                setsockopt(c, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
                struct epoll_event e = {.events = EPOLLIN, .data.fd = c};
                epoll_ctl(ep, EPOLL_CTL_ADD, c, &e);
                continue;
            }
            ssize_t m = recv(fd, buf, 65536, 0);
            if (m <= 0) { close(fd); continue; }
            ssize_t off = 0;
            while (off < m) {
                ssize_t w = send(fd, buf + off, m - off, MSG_NOSIGNAL);
                if (w <= 0) break;
                off += w;
            }
        }
    }
    return NULL;
}

struct cli {
    int fd;
    long ops;
    char pad[64];
};

static void *client_thread(void *arg) {
    struct cli *c = arg;
    char frame[64];
    memset(frame, 7, sizeof frame);
    long local = 0;
    while (!g_stop) {
        if (write(c->fd, frame, 8) != 8) break;
        if (read(c->fd, frame, sizeof frame) < 8) break;
        local++;
    }
    c->ops = local;
    return NULL;
}

int main(int argc, char **argv) {
    int cpu = argc > 1 ? atoi(argv[1]) : 1;
    int conns = argc > 2 ? atoi(argv[2]) : 64;
    g_secs = argc > 3 ? atoi(argv[3]) : 3;

    srv_fd = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    setsockopt(srv_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_in a = {0};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port = htons(0);
    bind(srv_fd, (struct sockaddr *)&a, sizeof a);
    socklen_t al = sizeof a;
    getsockname(srv_fd, (struct sockaddr *)&a, &al);
    listen(srv_fd, 1024);

    pthread_t st;
    pthread_create(&st, NULL, server_thread, NULL);
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    pthread_setaffinity_np(st, sizeof set, &set);

    usleep(200 * 1000);

    struct cli *cs = calloc(conns, sizeof *cs);
    pthread_t *ct = calloc(conns, sizeof *ct);
    for (int i = 0; i < conns; i++) {
        int c = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in b = {0};
        b.sin_family = AF_INET;
        b.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        /* sin_port 从 getsockname 拿回来时已经是网络序，再 htons 一次就连到别的端口上去了 */
        b.sin_port = a.sin_port;
        if (connect(c, (struct sockaddr *)&b, sizeof b) < 0) { perror("connect"); return 1; }
        setsockopt(c, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
        cs[i].fd = c;
    }
    for (int i = 0; i < conns; i++) pthread_create(&ct[i], NULL, client_thread, &cs[i]);

    double t0 = now_s();
    sleep(g_secs);
    g_stop = 1;
    for (int i = 0; i < conns; i++) pthread_join(ct[i], NULL);
    double span = now_s() - t0;

    long total = 0;
    for (int i = 0; i < conns; i++) total += cs[i].ops;
    printf("lb_floor cpu=%d conns=%d secs=%.2f ops=%ld -> %.0f ops/s  (%.2f us/op)\n",
           cpu, conns, span, total, total / span, span * 1e6 / (total ? total : 1));
    return 0;
}
