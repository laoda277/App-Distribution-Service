#pragma once

#include <fcntl.h>

// 所有要进 epoll 的 socket 都必须过这一道。
// 阻塞 socket 一旦在 EPOLLIN 里多 recv 一次，就会把 worker 线程睡死，
// 6 个 worker 睡满 = 整个服务装死。
inline bool setFdNonBlocking(int fd)
{
    int flags = ::fcntl(fd, F_GETFL, 0);
    if(flags < 0)
    {
        return false;
    }
    return ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}
