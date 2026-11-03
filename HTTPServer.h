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
   std::string dirRoot = "./www";
   std::string dataRoot = "./data";    // 业务数据根
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

   struct SendFileState{
      std::shared_ptr<FileFdOwner> file;   // 引用计数 保证引用期间不会被释放
      off_t  offset    = 0;               // 已经发出去多少字节
      size_t remaining = 0;               // 还剩多少字节
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


   enum class ReadStatus {Request, WaitMore, Closed, Error, TooLarge};

   ReadStatus readRequest(int clientSocket, std::string& connBuffer, std::string& request);
   std::string extractVersion(const std::string& request);
   bool clientWantsClose(const std::string& request);
   std::string httpDate(std::time_t t); //时间格式化
   std::string extractHeader(const std::string& request, const std::string& name); //读请求头
   std::string createNotModified(bool keepAlive, std::time_t mtime); //拼304响应
   std::string extractPath(const std::string& request);
   std::string createResponse(const std::string& content, const std::string& contentType = "text/html", int statusCode = 200, bool keepAlive = true, 
                                          const struct timespec* mtime = nullptr);
   std::string createDownloadHeader(const std::string& contentType,
                                             long long contentLength,
                                             int statusCode, bool keepAlive,
                                             const struct timespec* mtime = nullptr);

   void dispatchNewConnection(int clientFd);   // accept拿到fd后派发
   void dispatchReady(int fd, uint32_t events); // 就绪事件派发
   bool armOn(int epfd, int fd, uint32_t events); // 往指定 epoll 重新武装
   void removeOn(int epfd, int fd);             // 从指定 epoll 摘除
   void handleRedisEvent(uint32_t events);
   void checkRatelimit(const std::string& ip, long long need,
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
                                  bool keepAlive);
   void sendFileBody(int fd);
   void saveSendFileProgress(int fd, off_t offset, size_t remaining);
   bool writeAllToDisk(int fd, const char* data, size_t len);
   std::string sanitizeFileName(const std::string& raw);
   std::string extensionOf(const std::string& name);
   bool isUploadRequest(const std::string& request);
   long long parseContentLength(const std::string& request);
   void handleDownload(int fd, const std::string& userPath, bool keepAlive);
   bool beginRecv(int fd, const std::string& request, std::string& buf);
   void continueRecv(int fd);
   bool finishRecv(int fd, std::shared_ptr<RecvFileState> st);
};
