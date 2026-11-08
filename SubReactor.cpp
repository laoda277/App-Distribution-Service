#include "SubReactor.h"
#include "NetUtil.h"

#include <cerrno>
#include <utility>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

bool SubReactor::ctl(int fd, uint32_t events, int op)
{
    epoll_event ev{};
    ev.events = events | EPOLLONESHOT;
    ev.data.fd = fd;

    return epoll_ctl(epollFd_, op, fd, &ev) == 0;
}

bool SubReactor::start(WorkingHandler onWorking)
{
    if(thread_.joinable())
    {
        return false;
    }

    epollFd_ = epoll_create1(EPOLL_CLOEXEC);
    if(epollFd_ < 0)
    {
        return false;
    }

    wakeupFd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if(wakeupFd_ < 0)
    {
        close(epollFd_);
        epollFd_ = -1;
        return false;
    }

    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = wakeupFd_;

    if(epoll_ctl(epollFd_, EPOLL_CTL_ADD, wakeupFd_, &ev) != 0)
    {
        close(wakeupFd_);
        close(epollFd_);
        wakeupFd_ = -1;
        epollFd_ = -1;
        return false;
    }

    onWorking_ = std::move(onWorking);
    running_.store(true, std::memory_order_relaxed);
    thread_ = std::thread(&SubReactor::loop, this);

    return true;
}

void SubReactor::loop()
{
    epoll_event evs[kMaxEvents];

    while(running_.load(std::memory_order_relaxed))
    {
        int n = epoll_wait(epollFd_, evs, kMaxEvents, 1000);

        if(n < 0)
        {
            if(errno == EINTR)
            {
                continue;
            }
            break;
        }

        for(int i = 0; i < n; ++i)
        {

            int fd = evs[i].data.fd;

            if(fd == wakeupFd_){
            uint64_t v = 0;
            ssize_t r = read(wakeupFd_, &v, sizeof(v));
            (void)r;

            std::queue<std::function<void()>> tasks;  //搬发送命令
            {
                std::lock_guard<std::mutex> lk(pendingMutex_);
                tasks.swap(pending_);      
            }

            while(!tasks.empty()){
                tasks.front()();  //发送redis命令
                tasks.pop();
            }
            continue;
            }

            if(!onWorking_)
            {
                continue;
            }

            try  //统一执行所有任务
            {
                onWorking_(fd, evs[i].events);
            }
            catch(...)
            {
            }
        }
    }
}

bool SubReactor::addConnection(int fd)
{
    if(!setFdNonBlocking(fd))
    {
        return false;
    }

    if(!ctl(fd, EPOLLIN, EPOLL_CTL_ADD))
    {
        return false;
    }

    connCount_.fetch_add(1, std::memory_order_relaxed);

    return true;
}

bool SubReactor::arm(int fd, uint32_t events)
{
    return ctl(fd, events, EPOLL_CTL_MOD);
}

void SubReactor::removeConnection(int fd)
{
    if(ctl(fd, 0, EPOLL_CTL_DEL))
    {
        connCount_.fetch_sub(1, std::memory_order_relaxed);
    }
}

void SubReactor::post(std::function<void()> fn){ //任务入队并唤醒
   {
      std::lock_guard<std::mutex> lk(pendingMutex_);
      pending_.push(std::move(fn));
   }

   uint64_t one = 1;
   ssize_t r = write(wakeupFd_, &one, sizeof(one));
   (void)r;   // eventfd 计数溢出才会失败，忽略
}

void SubReactor::stop()
{
    bool expected = true;
    if(!running_.compare_exchange_strong(expected, false))
    {
        return;
    }

    uint64_t one = 1;
    ssize_t r = write(wakeupFd_, &one, sizeof(one));
    (void)r;
}

void SubReactor::join()
{
    if(thread_.joinable())
    {
        thread_.join();
    }
}

SubReactor::~SubReactor()
{
    stop();
    join();

    if(wakeupFd_ >= 0)
    {
        close(wakeupFd_);
        wakeupFd_ = -1;
    }

    if(epollFd_ >= 0)
    {
        close(epollFd_);
        epollFd_ = -1;
    }
}
