# App Distribution Service

面向内测包分发的 HTTP 服务。C++17 从零实现，不依赖任何 Web 框架 —— 事件循环、HTTP 协议解析、路由、连接生命周期全部自己写。

## 架构

```
                      ┌──────────────┐
     client ────────▶ │  MainReactor │  只做 accept
                      └──────┬───────┘
                             │ 轮询分发
                ┌────────────┴────────────┐
                ▼                         ▼
        ┌──────────────┐          ┌──────────────┐
        │ SubReactor 0 │          │ SubReactor 1 │   各持一个 epoll
        └──────┬───────┘          └──────┬───────┘
               └────────────┬────────────┘
                            ▼
                   ┌────────────────┐
                   │   ThreadPool   │  按「就绪事件」调度，不绑定连接
                   └────────────────┘
```

所有 fd 注册 `EPOLLONESHOT`，同一个连接在任一时刻只会有一个线程处理，处理完用 `EPOLL_CTL_MOD` 重新武装。工作线程不持有连接状态，同一个 fd 的两次请求可能落在不同线程上。

## 功能

| 功能       | 实现                                                                                                |
| -------- | ------------------------------------------------------------------------------------------------- |
| **上传**   | `PUT /upload/<filename>`，边收边算 SHA256，先写临时文件 → `fsync` → `rename` 原子落盘，断电不会留下半截文件；以内容哈希去重，重复上传直接秒传 |
| **下载**   | `GET /download/apps/<sha256><ext>`，`sendfile` 零拷贝不经用户态，支持 `Range` 断点续传（206），单连接令牌桶限速 10 MB/s      |
| **版本分发** | `GET /pkg/{product}/{version}?channel=` 查表后 302 到内容直链，URL 用内容哈希保证永久有效，换包只改映射不动链接                  |
| **限流**   | Redis + Lua 令牌桶，取令牌/结算/设过期在一段脚本里原子执行，下载与 `/pkg/` 各自独立配额互不占用                                       |
| **数据层**  | MySQL 三表拆分：`files` 存内容（sha256 唯一）、`releases` 存版本引用、`download_logs` 存流水；日志由独立线程异步批量落库              |

## 编译运行

依赖 g++（C++17）、libmysqlclient、OpenSSL、hiredis。

```bash
make
./httpserver 8080        # 端口可省略，默认 8080
```

hiredis 路径在 Makefile 的 `HIREDIS_DIR` 里配置。

## 性能

loopback 环境，100 MB 随机内容包体：

- 上传 **398 ms**（**264 MB/s**，含边收边算 SHA256）
- 单连接下载 **11.6 MB/s**（配置为 10 MB/s 限速 + 12 MB burst）
- 20 并发下载聚合 **226 MB/s**
- 静态文件 wrk `-t4 -c50` 约 **27 万 QPS / 1.87 GB/s**

## 已知限制

- 只有明文 HTTP，无 TLS
- 不支持 chunked 传输编码（上传需要 `Content-Length`）
- 只有连接空闲超时，没有请求总时长超时
- 单机单进程，无集群与水平扩展

---

早期版本（单 Reactor 静态文件服务器）：[cpp-http-server](https://github.com/laoda277/cpp-http-server)
