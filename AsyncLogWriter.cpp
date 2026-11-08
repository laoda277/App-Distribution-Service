#include "AsyncLogWriter.h"
#include "MySQLClient.h"

#include <algorithm>
#include <chrono>
#include <ctime>
#include <iostream>
#include <string>

namespace{

std::string formatTs(long long ts){  //把时间戳(数字串) 转为格式字符串
   std::time_t t = static_cast<std::time_t>(ts);
   std::tm tm{};
   localtime_r(&t, &tm);
   char buf[32];
   std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm);
   return std::string(buf);
}
}  

AsyncLogWriter::AsyncLogWriter(){
   // 单开线程 独立于线程池 
   thread_ = std::thread(&AsyncLogWriter::run, this);
}

AsyncLogWriter::~AsyncLogWriter(){
   {
      std::lock_guard<std::mutex> lk(mutex_);
      running_ = false;
   }
   cv_.notify_all();
   if(thread_.joinable()) thread_.join();

   const long long d = dropped_.load(std::memory_order_relaxed);
   if(d > 0){
      std::cerr << "下载日志队列满，累计丢弃 " << d << " 条\n";
   }
}

void AsyncLogWriter::write(const std::string& sha256, const std::string& ip,
                           long long bytes, int status){
   Entry e;
   e.sha256 = sha256;
   e.ip     = ip;
   e.bytes  = bytes;
   e.status = status;
   e.ts     = static_cast<long long>(std::time(nullptr));

   {
      std::lock_guard<std::mutex> lk(mutex_);
      if(queue_.size() >= kQueueMax){
         // 满了就丢，绝不阻塞下载线程。但丢掉要记账 —— 只丢不数，
         // 等于把「日志跟不上」这件事永久藏起来。
         dropped_.fetch_add(1, std::memory_order_relaxed);
         return;
      }
      queue_.push_back(std::move(e));
   }
   cv_.notify_one();
}

std::size_t AsyncLogWriter::pending() const{
   std::lock_guard<std::mutex> lk(mutex_);
   return queue_.size();
}

void AsyncLogWriter::run(){
   while(true){
      std::vector<Entry> batch;
      {
         std::unique_lock<std::mutex> lk(mutex_);
         cv_.wait_for(lk, std::chrono::milliseconds(kFlushMs), [this]{  //第一个参数：等待200ms 每次waitfor会重新计算now+200ms得时间戳
            return !queue_.empty() || !running_;
         });     //两个条件满足一个就可以

         const std::size_t n = std::min(queue_.size(), kBatchMax);  //最多取出100个 batch是这次循环实际执行的 queue里是所有需要执行的
         batch.reserve(n);
         for(std::size_t i = 0; i < n; ++i){
            batch.push_back(std::move(queue_.front()));
            queue_.pop_front();
         }

         if(batch.empty() && !running_) break;   // 队列空了而且收到停止信号
      }
      // 出了锁再做 IO —— 在锁里写库会把所有生产者线程一起卡住
      if(!batch.empty()) flush(batch);
      prune();   // 内部 60 秒节流，一次只删一批；失败不影响日志继续写
   }
}

void AsyncLogWriter::flush(const std::vector<Entry>& batch){  //记录入库
   if(batch.empty()) return;

   MySQLClient& db = mysqlClient();
   std::string sql =
      "INSERT INTO download_logs (sha256, ip, bytes, status, created_at) VALUES ";
   for(std::size_t i = 0; i < batch.size(); ++i){
      if(i > 0) sql += ',';
      // sha 来自磁盘路径、ip 来自对端地址，两个都在攻击者手里，必须 escape
      sql += "('" + db.escape(batch[i].sha256) + "','"
                 + db.escape(batch[i].ip)     + "',"
                 + std::to_string(batch[i].bytes)  + ','
                 + std::to_string(batch[i].status) + ",'"
                 + formatTs(batch[i].ts) + "')";
   }

   if(!db.execute(sql)){
      // 失败不重试：重试会把队列重新填满，日志服务的故障会传染给业务
      std::cerr << "下载日志落库失败: " << db.errorText() << '\n';
   }
}

void AsyncLogWriter::prune(){
   // 60 秒节流：run() 每轮都会调到这里，绝大多数调用直接返回
   const auto now = std::chrono::steady_clock::now();
   if(now - lastPrune_ < std::chrono::milliseconds(kPruneIntervalMs)) return;
   lastPrune_ = now;

   // created_at 是范围条件，必须靠 idx_time(created_at) 这根单列索引。
   // 已有的 idx_sha_time(sha256, created_at) 最左前缀是 sha256，这里用不上 —— 会退化成全表扫。
   // LIMIT 保证单条语句的影响行数和锁范围可控，删不完下一轮接着删。
   const std::string sql =
      "DELETE FROM download_logs WHERE created_at < NOW() - INTERVAL "
      + std::to_string(kRetentionDays) + " DAY LIMIT " + std::to_string(kPruneBatch);

   MySQLClient& db = mysqlClient();
   if(!db.execute(sql)){
      // 和 flush 同一个原则：失败不重试、不抛、不阻塞，清理失败不能影响日志继续写
      std::cerr << "下载日志清理失败: " << db.errorText() << '\n';
      return;
   }

   const long long n = static_cast<long long>(db.affectedRows());
   if(n > 0) pruned_.fetch_add(n, std::memory_order_relaxed);
}
