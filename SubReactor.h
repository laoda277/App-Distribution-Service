#pragma once
#include <thread>
#include <atomic>
#include <functional>
#include <cstdint>
#include <cerrno>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

class SubReactor{
public:
   using WorkingHandler = std::function<void(int, uint32_t)>;

   SubReactor() = default;
   ~SubReactor();

   bool start(WorkingHandler onWorking);
   void stop();
   void join();

   bool addConnection(int fd);
   bool arm(int fd, uint32_t events);
   void removeConnection(int fd);

   int epollFd() const { return epollFd_; }
   size_t connectionCount() const { return connCount_.load(std::memory_order_relaxed); }

private:
   void loop();
   bool ctl(int fd, uint32_t events, int op);

   int epollFd_ = -1;
   int wakeupFd_ = -1;
   std::thread thread_;
   std::atomic<bool> running_{false};
   std::atomic<size_t> connCount_{0};
   WorkingHandler onWorking_;
   static constexpr int kMaxEvents = 16;
};