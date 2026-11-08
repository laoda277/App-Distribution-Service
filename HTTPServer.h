#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <chrono>
#include <atomic>
#include <cstdint>
#include <memory>
#include <vector>
#include <functional>
#include <openssl/sha.h>


#include "ThreadPool.h"
#include "EpollEngine.h"
#include "MimeTypes.h"
#include "FileCache.h"
#include "SubReactor.h"
#include "RedisAsyncClient.h"
#include "AsyncLogWriter.h"


class HTTPServer{
public:
   explicit HTTPServer(int port);
   ~HTTPServer();

   bool start();
   void stop();

private:
   int port;
   bool running;
   bool initialized;
   bool shutDown_ = false;
   std::unordered_map<int, std::string> connBuffers_;
   std::unordered_map<int, std::string> outBuffers_;
   std::mutex connMutex_;
   std::unordered_map<int, std::chrono::steady_clock::time_point> lastActive_;
   std::unordered_set<int> inFlight_; //work是否在占着这个fd工作
   std::unordered_set<int> pendingConn_; //异步正在处理的fd
   std::unordered_set<int> closeAfterWrite_; //还没写完但想关闭的fd 写完才能关闭 即短链接
   std::unordered_map<int, uint64_t> connGen_;   // fd → 世代号
   std::chrono::steady_clock::time_point lastSweep_;
   static constexpr std::chrono::seconds kIdleTimeout{30};
   static constexpr long long kMaxUploadSize = 2LL * 1024 * 1024 * 1024;   // 单次上传上限 2GB
   static constexpr size_t    kRecvChunk     = 64 * 1024;                  // 每次 recv 64KB
   static constexpr size_t kSendRateBytesPerSec = 10u*1024*1024; // 单连接 10MB/s
   static constexpr size_t kSendBurstBytes      = 12u * 1024 * 1024;  // ≥ 1 秒补量，留 20% 余量抗 tick 抖动
   std::string dirRoot = "./www";
   std::string dataRoot = "./data";    // 业务数据根
   AsyncLogWriter logWriter_;  
   ThreadPool threadPool;
   EpollEngine epollEngine;
   MimeTypes mimeTypes;
   FileCache fileCache;
   RedisAsyncClient redis_;
   std::vector<std::unique_ptr<SubReactor>> subReactors_;
   std::unordered_map<int, int> fdEpoll_;  // fd → 归属的 epollFd_
   std::atomic<size_t> nextSub_{0};  //计数器      
   std::atomic<uint64_t> totalRequests_{0}; //记录总的有效请求次数
   std::atomic<uint64_t> cacheHits_{0}; //缓存get命中次数
   std::atomic<uint64_t> cacheMisses_{0}; //缓存get未命中次数
   std::atomic<int> redisFd_{-1};
   std::atomic<uint64_t> genCounter_{0};          // 全局递增，保证绝不重复
   std::string      ratelimitSha_;
   std::chrono::steady_clock::time_point startTime_{std::chrono::steady_clock::now()}; //服务器开启的时刻
   static constexpr size_t kSubReactorCount = 2;

   struct PendingGuard{ //控制pending表的擦除
   HTTPServer* self;
   int      fd;
   uint64_t gen;        // 必须带：fd 号会复用，只有 gen 能区分「还是不是我那条连接」

   PendingGuard(HTTPServer* s, int f, uint64_t g) : self(s), fd(f), gen(g) {}
   ~PendingGuard(){
      // 不抛异常，纯清理
      std::lock_guard<std::mutex> lk(self->connMutex_);
      auto it = self->connGen_.find(fd);
      // 只有「配对还成立」才动标记：要么正常结束，要么连接已关（此处不动，
      // closeConnection 已清过）。gen 不符 = fd 已复用成新连接，绝不能删它的豁免。
      if(it != self->connGen_.end() && it->second == gen){
         self->pendingConn_.erase(fd);
      }
   }

   PendingGuard(const PendingGuard&) = delete;
   PendingGuard& operator=(const PendingGuard&) = delete;
};

   struct FileFdOwner{  //可下载文件的句柄（位于磁盘）
      int fd = -1;
      explicit FileFdOwner(int f) : fd(f) {}
      ~FileFdOwner(){ if(fd >= 0) ::close(fd); }
      FileFdOwner(const FileFdOwner&) = delete;
      FileFdOwner& operator=(const FileFdOwner&) = delete;
      };
   
   struct ByteRange{
      long long start   = 0;
      long long end     = 0;       // 闭区间，两端都算进去
      bool      partial = false;   // false = 客户端没带 Range，整份发
   };

   enum class RangeParse{ None, Ok, Unsatisfiable };  //没有range，有，有但起点超过文件

   RangeParse parseRangeSpec(const std::string& value,
                           long long fileSize, ByteRange& out);


   struct SendBucket{        // 发送桶               
   size_t tokens = kSendBurstBytes;   //还能发多少
   std::chrono::steady_clock::time_point lastRefill = std::chrono::steady_clock::now();  //上一次补充时间
   };
   std::unordered_map<int, SendBucket> sendBuckets_;    // fd → 令牌桶


   struct SendFileState{
      std::shared_ptr<FileFdOwner> file;   // 引用计数 保证引用期间不会被释放
      off_t  offset    = 0;               // 已经发出去多少字节
      size_t remaining = 0;               // 还剩多少字节
      bool   throttled = false;                          // 额度耗尽，暂停中
   };
   std::unordered_map<int, SendFileState> sendFileStates_;   // sockfd → 发送进度


   struct TempFileOwner{  //用于用户上传场景
      int         fd = -1;
      std::string path;                 // 临时文件的磁盘路径
      TempFileOwner(int f, std::string p) : fd(f), path(std::move(p)) {}
      ~TempFileOwner(){
         if(fd >= 0) ::close(fd);
         if(!path.empty()) ::unlink(path.c_str()); //若上传成功则rename迁移走了 没成功则需要析构清理
      }

      void release(){ path.clear(); }  //rename成功后  不要unlink已经不用了的路径 防止重名危害

      TempFileOwner(const TempFileOwner&) = delete;
      TempFileOwner& operator=(const TempFileOwner&) = delete;
   };

   struct RecvFileState{
      std::shared_ptr<TempFileOwner> file;   // 临时文件，析构即 close + unlink
      uint64_t     gen       = 0;         // 世代号，防 fd 复用
      std::string  fileName;              // 客户端给的名字（已清洗）
      std::string  mime;                  // 入库用
      long long    remaining = 0;         // 还差多少字节没收
      long long    total     = 0;         // Content-Length 原值
      SHA256_CTX   sha{};                 // 增量哈希，边收边算，链式计算出已存入的文件的sha256
   };

   std::unordered_map<int, std::shared_ptr<RecvFileState>> recvFileStates_;  //socketfd->接受记录


   // Request 的含义：request 里已经是一条【完整】消息。
   //   普通请求：请求行 + 头 + body
   //   上传请求：只有请求行 + 头，body 原样留在 connBuffer 里交给 beginRecv 流式处理
   // 新增的 BadRequest / PayloadTooLarge 是为了让 readRequest 能把「为什么失败」带出来，
   // 由 handleOnce 映射成 400 / 413，不再一律 500。
   enum class ReadStatus {Request, WaitMore, Closed, Error, TooLarge, BadRequest, PayloadTooLarge};

   ReadStatus readRequest(int clientSocket, std::string& connBuffer, std::string& request);
   std::string extractVersion(const std::string& request);
   bool clientWantsClose(const std::string& request);
   std::string httpDate(std::time_t t); //时间格式化
   std::string extractHeader(const std::string& request, const std::string& name); //读请求头
   std::string createNotModified(bool keepAlive, std::time_t mtime); //拼304响应
   std::string extractPath(const std::string& request);
   bool methodIs(const std::string& request, const std::string& method); //判断请求行的动词是不是某个方法
   std::string createResponse(const std::string& content, const std::string& contentType = "text/html", int statusCode = 200, bool keepAlive = true, 
                                          const struct timespec* mtime = nullptr);
   std::string createDownloadHeader(const std::string& contentType,
                                             long long contentLength,
                                             int statusCode, bool keepAlive,
                                             const struct timespec* mtime = nullptr,
                                             long long rangeStart = -1,
                                             long long rangeEnd = -1, 
                                             long long totalSize = 0);

   void dispatchNewConnection(int clientFd);   // accept拿到fd后派发
   void dispatchReady(int fd, uint32_t events); // 就绪事件派发
   bool armOn(int epfd, int fd, uint32_t events); // 往指定 epoll 重新武装
   void removeOn(int epfd, int fd);             // 从指定 epoll 摘除
   void handleRedisEvent(uint32_t events);
   // key 和配额由调用方给：下载防带宽、/pkg/ 防刷库，两者的桶必须分开，
   // 否则点一次短链会吃掉两次下载配额。
   void checkRatelimit(const std::string& key, long long need,
                       long long capacity, long long rate,
                       std::function<void(bool ok, long long remain)> done);

   bool processRequest(int clientSocket, const std::string& request);
   void handleOnce(int clientSocket);
   void handleWritable(int fd);
   void finishOnce(int fd);
   void closeConnection(int fd);
   [[nodiscard]] bool trySend(int fd, const char*& data, size_t& toSend);
   bool sendResponse(int clientSocket, const std::string& response);
   void sweepIdleConnection();
   void shutdownServer();
   bool send404(int clientSocket, bool keepAlive = true);
   bool send500(int clientSocket, bool keepAlive = false);
   std::string getMimeType(const std::string& path);
   std::string readHTMLFile(const std::string& filename, struct timespec* outMtime = nullptr);
      void sendDownloadError(int fd, int code, const std::string& title, bool keepAlive);
   std::string peerIp(int fd); //对端ip
   void sendDownloadFile(int fd, uint64_t gen, const std::string& diskPath,
                                  bool keepAlive, const ByteRange& range);
   void sendFileBody(int fd);
   void saveSendFileProgress(int fd, off_t offset, size_t remaining, size_t tokens, bool throttled);
   bool writeAllToDisk(int fd, const char* data, size_t len);
   std::string sanitizeFileName(const std::string& raw);
   std::string extensionOf(const std::string& name);
   bool isUploadRequest(const std::string& request);
   long long parseContentLength(const std::string& request);
   void handleDownload(int fd, const std::string& userPath, bool keepAlive, const std::string& rangeHeader);
   bool handleCreateRelease(int fd, const std::string& request, bool keepAlive); //登记一个版本（POST /api/releases）
   bool handleListReleases(int fd, const std::string& query, bool keepAlive);
   bool handleListProducts(int fd, const std::string& query, bool keepAlive); //产品列表 GET /api/products
   void handlePkg(int fd, const std::string& userPath, const std::string& query, bool keepAlive);
   bool beginRecv(int fd, const std::string& request, std::string& buf);
   void continueRecv(int fd);
   bool finishRecv(int fd, std::shared_ptr<RecvFileState> st);
};
