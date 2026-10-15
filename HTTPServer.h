#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <chrono>
#include "ThreadPool.h"
#include "EpollEngine.h"
#include "MimeTypes.h"
#include "FileCache.h"
#include <atomic>
#include <cstdint>

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
   std::unordered_set<int> inFlight_;
   std::chrono::steady_clock::time_point lastSweep_;
   static constexpr std::chrono::seconds kIdleTimeout{30};
   std::string dirRoot = "./www";
   ThreadPool threadPool;
   EpollEngine epollEngine;
   MimeTypes mimeTypes;
   FileCache fileCache;
   std::atomic<uint64_t> totalRequests_{0}; //记录总的有效请求次数
   std::atomic<uint64_t> cacheHits_{0}; //缓存get命中次数
   std::atomic<uint64_t> cacheMisses_{0}; //缓存get未命中次数
   std::chrono::steady_clock::time_point startTime_{std::chrono::steady_clock::now()}; //服务器开启的时刻

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
};
