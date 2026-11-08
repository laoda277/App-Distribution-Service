#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

inline std::string shaFromPath(const std::string& diskPath){  //从磁盘路径提取sha
   const std::size_t slash = diskPath.find_last_of('/');
   const std::size_t begin = (slash == std::string::npos) ? 0 : slash + 1;
   const std::size_t dot   = diskPath.find('.', begin);
   if(dot == std::string::npos) return diskPath.substr(begin);
   return diskPath.substr(begin, dot - begin);
}

class AsyncLogWriter{
public:
   AsyncLogWriter();
   ~AsyncLogWriter();

   // 含 std::thread，拷贝必然 double join —— 和 MySQLClient 一个道理，直接禁死
   AsyncLogWriter(const AsyncLogWriter&) = delete;
   AsyncLogWriter& operator=(const AsyncLogWriter&) = delete;

   // 任意线程可调。队列满了直接丢，绝不阻塞调用者
   void write(const std::string& sha256, const std::string& ip,
              long long bytes, int status);

   long long   dropped() const { return dropped_.load(std::memory_order_relaxed); }
   long long   pruned()  const { return pruned_.load(std::memory_order_relaxed); }
   std::size_t pending() const;

private:
   struct Entry{
      std::string sha256;
      std::string ip;
      long long   bytes  = 0;
      int         status = 200;
      long long   ts     = 0;
   };

   void run();
   void flush(const std::vector<Entry>& batch);
   void prune();        // 保留策略：删掉超期日志，一次只删一批

   static constexpr std::size_t kQueueMax = 10000;
   static constexpr std::size_t kBatchMax = 100;
   static constexpr int     kFlushMs  = 200;

   // 只保留 30 天。DELETE 必须带 LIMIT 分批：一条语句删几十万行会一次性写满
   // undo log、放大锁范围、把主从延迟拉爆，purge 线程追不上还会撑大表空间。
   static constexpr int kRetentionDays   = 30;
   static constexpr int kPruneBatch      = 5000;
   static constexpr int kPruneIntervalMs = 60 * 1000;

   mutable std::mutex    mutex_;
   std::deque<Entry>     queue_;
   std::condition_variable cv_;
   std::thread        thread_;
   bool    running_ = true;   // 受 mutex_ 保护；构造即运行
   std::atomic<long long>  dropped_{0};
   std::atomic<long long>  pruned_{0};

   // 只有 run() 自己碰，不用同步
   std::chrono::steady_clock::time_point lastPrune_{};
};
