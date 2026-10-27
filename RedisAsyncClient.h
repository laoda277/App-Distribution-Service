#pragma once

#include <string>
#include <vector>
#include <functional>
#include <hiredis/hiredis.h>
#include <hiredis/async.h>
#include <cstdint>

class SubReactor;

class RedisAsyncClient{
public:
   // reply 为 nullptr 表示出错（连接断了 / 超时）
   using ReplyFn = std::function<void(redisReply*)>;
   using DisconnectFn = std::function<void()>;  //hiredis异步断开链接时候的回调函数

   RedisAsyncClient() = default;
   ~RedisAsyncClient();

   RedisAsyncClient(const RedisAsyncClient&) = delete;
   RedisAsyncClient& operator=(const RedisAsyncClient&) = delete;

   // 必须在 SubReactor 线程调用
   bool connect(const std::string& host, int port, SubReactor* sub, DisconnectFn onDisconnect = nullptr);
   void close();
   bool valid() const { return ac_ != nullptr; }
   int  fd() const;

   // epoll 报事件后由 SubReactor 线程调用
   void handleEvent(uint32_t events);

   // 下面三个都必须在 SubReactor 线程调用
   bool scriptLoad(const std::string& script, ReplyFn fn);
   bool evalBySha(const std::string& sha,
                const std::vector<std::string>& keys,
                const std::vector<std::string>& args,
                ReplyFn fn);

private:
   redisAsyncContext* ac_ = nullptr;  //一条链接的全部状态

   static void sOnDisconnect(const redisAsyncContext* ac, int status);
   DisconnectFn onDisconnect_; 
};