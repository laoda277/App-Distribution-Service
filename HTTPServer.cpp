#include <iostream>
#include <string>
#include <cstring>
#include <cerrno>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <cstdlib>  
#include <climits>   
#include <cctype>
#include <chrono>
#include <ctime>
#include <sys/stat.h>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <fcntl.h>
#include <future>
#include <memory>
#include <sys/sendfile.h>
#include<arpa/inet.h>
#include<netinet/in.h>
#include "HTTPServer.h"
   static epoll_event makeEvent(int fd, uint32_t events){
    epoll_event ev{};
    ev.events = events | EPOLLONESHOT;
    ev.data.fd = fd;
    return ev;
   }

   static constexpr long long kRatelimitCapacity = 100LL * 1024 * 1024;  // 桶容量 100MB 也就是cap
   static constexpr long long kRatelimitRate     =  10LL * 1024 * 1024;  // 每秒补 10MB 就是rate
   //令牌桶算法的lua脚本: 每个key对应一个ip 每个field有tok和ts两项 分别对应可用余量和上一次修改时间
   //余量的更新是懒加载的 并对于每个可以设计ttl防止无限膨胀
   static const char* kRatelimitScript = R"LUA(
   local t    = redis.call('HMGET', KEYS[1], 'tok', 'ts')
   local tok  = tonumber(t[1])   
   local ts   = tonumber(t[2])
   local now  = tonumber(ARGV[1])
   local cap  = tonumber(ARGV[2])
   local rate = tonumber(ARGV[3])
   local need = tonumber(ARGV[4])
   local ttl  = math.ceil(cap / rate * 2)

   if tok == nil then tok, ts = cap, now end
   tok = math.min(cap, tok + math.max(0, now - ts) * rate)

   if tok < need then
   redis.call('HMSET', KEYS[1], 'tok', tok, 'ts', now)
   redis.call('EXPIRE', KEYS[1], ttl)
   return -1
   end

   tok = tok - need
   redis.call('HMSET', KEYS[1], 'tok', tok, 'ts', now)
   redis.call('EXPIRE', KEYS[1], ttl)
   return math.floor(tok)
   )LUA";

   HTTPServer::ReadStatus HTTPServer::readRequest(int clientSocket,std::string& connBuffer, std::string& request){ //将缓冲区内容写到request中 返回值表示读的状态
       const size_t kMaxTotal = 8192;
       char chunk[4096];

       while(true){
         size_t pos = connBuffer.find("\r\n\r\n");
         if(pos != std::string::npos){
            request = connBuffer.substr(0, pos+4);
            connBuffer.erase(0, pos+4);
            return ReadStatus::Request;
         }

         if(connBuffer.size() >= kMaxTotal) return ReadStatus::TooLarge;

         ssize_t n;
         do{
            n = recv(clientSocket, chunk, sizeof(chunk), 0);
         }while(n<0 && errno == EINTR);

         if(n < 0){
            if(errno == EAGAIN || errno == EWOULDBLOCK)
            return ReadStatus::WaitMore;
         return ReadStatus::Error;
         }
         if(n == 0){
            return ReadStatus::Closed;
         }
         connBuffer.append(chunk, (size_t)n);      
       }
   }

   std::string HTTPServer::extractVersion(const std::string& request){
      size_t lineEnd = request.find("\r\n");
      std::string line = request.substr(0, lineEnd);

      size_t sp = line.rfind("/");
      if(sp == std::string::npos) return "";
      return line.substr(sp+1);
   }

   bool HTTPServer::clientWantsClose(const std::string& request){
      std::string lower = request;
      std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c){
         return std::tolower(c);
      });
      size_t p = lower.find("\r\nconnection:");
      if(p != std::string::npos) {
         size_t e = lower.find("\r\n", p+2);

         if(e != std::string::npos){
            std::string val = lower.substr(p+2, e-(p+2));
            if(val.find("close") != std::string::npos) return true;
            if(val.find("keep-alive") != std::string::npos)  return false;
         }
      }
      return extractVersion(request) == "1.0";
      
   }
 
   std::string HTTPServer::httpDate(std::time_t t){
      struct tm tmv;
      std::memset(&tmv, 0, sizeof(tmv)); //初始化值为0
      gmtime_r(&t, &tmv);  //把前一个时间戳转化为UTC格式并存入后者结构体
      char buf[64];
      std::strftime(buf, sizeof(buf), "%a, %d %b %Y %H:%M:%S GMT", &tmv); //把tmv用第三个参数的格式输出到buf中
      return std::string(buf);
   }

   std::string HTTPServer::extractHeader(const std::string& request, const std::string& name){ //输入想要提取的头 输出头的值 name必须传小写因为是在lower里操作的
      std::string lower = request;
      std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c){
         return std::tolower(c);
      });

      std::string key = "\r\n" + name + ":"; //保证是真的头部
      size_t p = lower.find(key);
      if(p == std::string::npos) return "";

      size_t valStart = p + key.size(); //值开始的地方
      while(valStart < request.size() && request[valStart] == ' ') ++valStart;  //让valStart跳过空格一直走到真正的字符串开始的地方
      size_t valEnd = request.find("\r\n", valStart);   //找到下一个头的前面位置或者说是本响应体结束的地方
      if(valEnd == std::string::npos) return"";
      while(valEnd > valStart && (request[valEnd - 1] == ' ' || request[valEnd -1] == '\t')) valEnd--;  //抹除末尾的空格和制表符（保证时间戳精确匹配）
      return request.substr(valStart, valEnd - valStart);  //从原串切而不是lower
   }

      std::string HTTPServer::extractPath(const std::string& request){
      size_t start=request.find(" ");
      if(start==std::string::npos) return"/";

      size_t end=request.find(" ",start+1);
      if(end==std::string::npos)  return "/";

      return request.substr(start+1,end-start-1);
      } 


      std::string HTTPServer::createResponse(const std::string& content, const std::string& contentType,
                                                      int statusCode, bool keepAlive, const struct timespec* mtime) { 
       std::ostringstream response;
       std::string statusMessage;
       if(statusCode==200){
         statusMessage="OK";
       }
       else if(statusCode==404){
         statusMessage="Not Found";
       }
       else if(statusCode==500){
         statusMessage="Internal Server Error";
       }
       else if(statusCode==429){
          statusMessage="Too Many Requests"; 
       }
       else{
         statusMessage="Unknown";
       }

       response << "HTTP/1.1 " << statusCode << " "<< statusMessage << "\r\n";
       response << "Date: " << httpDate(std::time(nullptr)) << "\r\n";   //响应头创建的时间 time函数返回值是现在的时刻 参数是通过引用传递然后出参的方式返回时刻 
                                                                        //也就是有两个出口但只要一个
       response << "Content-Type: " << contentType << "\r\n";
       if(mtime != nullptr){   //只有在传入mtime的时候才需要  在发送错误页的时候不会传入时间戳
         response << "Last-Modified: " << httpDate(mtime->tv_sec) << "\r\n"; //发送最后更改时间
       }
       response << "Cache-Control: no-cache\r\n";  //要求客户端可以缓存 但每次需要先询问  no-stroe才是不让缓存
       response << "Content-length: " << content.length() << "\r\n";
       response << "Connection: " << (keepAlive? "keep-alive": "close") << "\r\n";
       response << "\r\n";
       response << content;

       return response.str();
      }


      std::string HTTPServer::createDownloadHeader(const std::string& contentType,
                                             long long contentLength,
                                             int statusCode, bool keepAlive,
                                             const struct timespec* mtime){   // 默认值只写在 .h
      // 状态码 → 原因短语。老 createResponse 只认 200/404/500，429 会被写成 "Unknown"
         const char* statusMessage = "Unknown";
         switch(statusCode){
            case 200: statusMessage = "OK";                    break;
            case 400: statusMessage = "Bad Request";           break;
            case 403: statusMessage = "Forbidden";             break;
            case 404: statusMessage = "Not Found";             break;
            case 413: statusMessage = "Payload Too Large";     break;
            case 429: statusMessage = "Too Many Requests";     break;
            case 500: statusMessage = "Internal Server Error"; break;
         }

         std::ostringstream response;
         response << "HTTP/1.1 " << statusCode << " " << statusMessage << "\r\n";
         response << "Date: " << httpDate(std::time(nullptr)) << "\r\n";
         response << "Content-Type: " << contentType << "\r\n";
         if(mtime != nullptr){                                          //  和 createResponse 保持同样的能力
            response << "Last-Modified: " << httpDate(mtime->tv_sec) << "\r\n";
         }
         response << "Cache-Control: no-cache\r\n";
         response << "Content-length: " << contentLength << "\r\n";   //  用参数，不是 body 长度
         response << "Connection: " << (keepAlive ? "keep-alive" : "close") << "\r\n";
         response << "\r\n";                                          // 空行之后就是文件体
         return response.str();
      }

      void HTTPServer::sendDownloadError(int fd, int code, const std::string& title, bool keepAlive){
         std::string body = "<html><body><h1>" + title + "</h1></body></html>";
         if(!sendResponse(fd, createResponse(body, "text/html", code, keepAlive))){
            closeConnection(fd);
            return;
         }
         finishOnce(fd);     // 若还有残留，finishOnce 会自动 arm EPOLLOUT
      }


      std::string HTTPServer::createNotModified(bool keepAlive, std::time_t mtime){  //304响应 表示客户端自上次访问并未改变 让客户端用缓存的内容
         std::ostringstream response;
            response << "HTTP/1.1 304 Not Modified\r\n";  //通过该行让客户端知道
            response << "Date: " << httpDate(std::time(nullptr)) << "\r\n";
            response << "Cache-Control: no-cache\r\n";
            response << "Last-Modified: " << httpDate(mtime) << "\r\n";
            response << "Connection: " << (keepAlive ? "keep-alive" : "close") << "\r\n";
            response << "\r\n";
         return response.str();   // 到空行为止 没有 Content-Length 没有 Content-Type 没有正文
}
      
      bool HTTPServer::processRequest(int clientSocket, const std::string& request){ 
         bool keep = !clientWantsClose(request); //表示是否还保持长连接
         std::string userPath = extractPath(request);  
         totalRequests_.fetch_add(1, std::memory_order_relaxed); //最宽松约束 无需关注顺序

         if(userPath == "/" || userPath .empty()){
            userPath = "/DefaultPage.html";
         }
         if(userPath == "/about") {
            userPath = "/AboutPage.html";
         }
         if(userPath == "/api/status") {
            size_t inFlight = 0;
            {
               std::lock_guard<std::mutex> lk(connMutex_);
               inFlight = inFlight_.size();
            }
            auto uptime = std::chrono::duration_cast<std::chrono::seconds>( //duration是时间转换 把纳秒转为整秒
               std::chrono::steady_clock::now() - startTime_).count(); //计算到现在的时间 .count只要数据不用单位

               std::ostringstream body;
               body << "{" << "\"uptime_seconds\":"   << uptime     //转义符号\把"回归本意输出
                    << ",\"total_requests\":"   << totalRequests_.load(std::memory_order_relaxed) //读取的内存顺序要求
                    << ",\"cache_hits\":"    << cacheHits_.load(std::memory_order_relaxed)
                    << ",\"cache_misses\":"   << cacheMisses_.load(std::memory_order_relaxed)
                    << ",\"in_flight\":"   << inFlight
                    << ",\"connections_per_reactor\":[";
                     for(size_t i = 0; i < subReactors_.size(); ++i)
                     {
                        if(i > 0)
                        {
                           body << ",";
                        }

                        body << subReactors_[i]->connectionCount();
                     }

                     body << "]}";

            
         std::string response = createResponse(body.str(), "application/json", 200, keep);
         bool alive = sendResponse(clientSocket, response);
         return keep && alive;
            
         }
         if(userPath.compare(0, 10, "/download/") == 0){
            handleDownload(clientSocket, userPath, keep);
            return true;  //这里不管 关不关闭由closeAfterWrite_ 决定
         }

         char resolved[PATH_MAX];
         if(realpath((dirRoot + userPath).c_str(), resolved) == nullptr){ //转为绝对路径
            bool alive = send404(clientSocket, keep);
            return keep && alive; //前者是协议方面的是否关闭 后者是连接是否存活方面但连接为断开时默认值等于前者
         }
         std::string path = resolved;
         
         if(path.compare(0, dirRoot.size(), dirRoot) != 0 || (path.size() > dirRoot.size() && path[dirRoot.size()] != '/')){
            bool alive = send404(clientSocket, keep);
            return keep && alive;
         }
         
            std::string type = getMimeType(path);

            struct timespec mtime{};
            std::string content = readHTMLFile(path, &mtime); //获得最后一次的修改时间
         if(content.empty()){
            bool alive = send404(clientSocket, keep);
            return keep && alive;
         }

         std::string ims = extractHeader(request, "if-modified-since");
         if(!ims.empty() && ims == httpDate(mtime.tv_sec)){  //假如发送来的报文里的时间戳和本机时间戳相同
            std::string response = createNotModified(keep, mtime.tv_sec); //创建无需确认的报文
            bool alive = sendResponse(clientSocket, response);
            return keep && alive;
         }

         std::string response = createResponse(content, type, 200, keep, &mtime); 
         bool alive = sendResponse(clientSocket, response);
         return keep && alive; //同样sendResponse返回值表示连接是否存活 但中间没有send404这一层
         
      }

      void HTTPServer::handleOnce(int clientSocket){
         std::string buf;
         {
            std::lock_guard<std::mutex> lk(connMutex_);
            buf = std::move(connBuffers_[clientSocket]);
            lastActive_[clientSocket] = std::chrono::steady_clock::now();
            inFlight_.insert(clientSocket);
         }
         

         while(true){
            std::string request;
            ReadStatus st = readRequest(clientSocket, buf, request);

            if(st == ReadStatus::Request) {
               bool alive = true; //表示这个链接还能不能保持工作
               try{
                  alive = processRequest(clientSocket, request);
               }catch(...){
                  std::cerr<<"服务器出现异常"<<std::endl;
                  send500(clientSocket);
                  alive = false; 
               }
               if(!alive) {
                  closeConnection(clientSocket);
                  return;
               }
               continue;
            }
            if(st == ReadStatus::WaitMore) break;
            if(st == ReadStatus::TooLarge) send500(clientSocket);
            closeConnection(clientSocket);
            return;
         }
         {
            std::lock_guard<std::mutex> lk(connMutex_);
            connBuffers_[clientSocket] = std::move(buf);
         }
         finishOnce(clientSocket);

}

      void HTTPServer::handleWritable(int fd){
         std::string out;
         bool hasSendFile = false;
         {
            std::lock_guard<std::mutex> lk(connMutex_);
            lastActive_[fd] = std::chrono::steady_clock::now();
            inFlight_.insert(fd);
            auto it = outBuffers_.find(fd);
            if(it != outBuffers_.end()){
               out = std::move(it -> second);
               outBuffers_.erase(it);
            }
            hasSendFile = (sendFileStates_.count(fd) > 0);
         }

         if(!out.empty()){
         const char* data = out.data();
         size_t toSend = out.size();
         if(!trySend(fd, data, toSend)){
            closeConnection(fd);
            return;
         }
         if(toSend > 0){
            {
            std::lock_guard<std::mutex> lk(connMutex_);
            outBuffers_[fd].append(data, toSend);
            }
         finishOnce(fd);
         return;
               }
          }
          
          if(hasSendFile){
            sendFileBody(fd);
            return;  //让sendFileBody自己arm
          }
          bool closing = false; //判断是否需要关闭 此时已经发送完毕
          {
            std::lock_guard<std::mutex> lk(connMutex_);
            auto it = closeAfterWrite_.find(fd);
            if(it != closeAfterWrite_.end()){
               closeAfterWrite_.erase(it);
               closing = true;
            }
          }
          if(closing){
            closeConnection(fd);
            return;
          }
          finishOnce(fd);

      }


      void HTTPServer::finishOnce(int fd)
         {
            bool hasPending = false;
            int epfd = -1;

            {
               std::lock_guard<std::mutex> lk(connMutex_);
               inFlight_.erase(fd);

               auto outIt = outBuffers_.find(fd);
               if(outIt != outBuffers_.end() && !outIt->second.empty())
               {
                     hasPending = true;
               }

               auto sendfileIt = sendFileStates_.find(fd);
               if(sendfileIt != sendFileStates_.end() && sendfileIt ->second.remaining > 0)
               {
                  hasPending = true;
               }

               auto epIt = fdEpoll_.find(fd);
               if(epIt != fdEpoll_.end())
               {
                     epfd = epIt->second;
               }
            }

            if(epfd >= 0)
            {
               armOn(epfd, fd, hasPending ? EPOLLOUT : EPOLLIN);
            }
         }

      


      void HTTPServer::closeConnection(int fd){
         int epfd = -1;

         {
            std::lock_guard<std::mutex> lk(connMutex_);
            auto it = fdEpoll_.find(fd);
            if(it != fdEpoll_.end())
            {
                  epfd = it->second;
                  fdEpoll_.erase(it);
                  connGen_.erase(fd);
            }
         }

         if(epfd >= 0)
         {
            removeOn(epfd, fd);
         }
         {
            std::lock_guard<std::mutex> lk(connMutex_);
            connBuffers_.erase(fd);
            outBuffers_.erase(fd);
            lastActive_.erase(fd);
            inFlight_.erase(fd);
            pendingConn_.erase(fd);
            closeAfterWrite_.erase(fd);
            sendFileStates_.erase(fd);
         }
         close(fd);
      }


      [[nodiscard]] bool HTTPServer::trySend(int fd, const char*& data, size_t& toSend){ //尝试进行发送
         while(toSend > 0){
            ssize_t sent = send(fd, data, toSend, 0);
            if(sent < 0){
               if(errno == EINTR) continue;  //暂时中断
               if(errno == EAGAIN || errno == EWOULDBLOCK) return true; //两个为同一个意思 表示缓冲区已满或者没有新数据
               return false; //连接已经断开
            }
            data += sent;
            toSend -= sent;
         }
         return true;
      }

      bool HTTPServer::sendResponse(int clientSocket, const std::string& response){
         const char* data = response.data(); //从第几个数据开始发送
         size_t toSend = response.size(); //还需要发送多少数据

         if(!trySend(clientSocket, data, toSend)){ //尝试发送一次，成功执行下一个if
            return false;
         }
         if(toSend > 0){
            std::lock_guard<std::mutex> lk(connMutex_);
            outBuffers_[clientSocket].append(data, toSend); //还没发送完，把剩余数据放回缓冲区
         }
         return true;
      }

      void HTTPServer::sweepIdleConnection(){
         auto now = std::chrono::steady_clock::now();
         {
            std::lock_guard<std::mutex> lk(connMutex_);
            if(now - lastSweep_ < std::chrono::seconds(1)) return;
            lastSweep_ =  now;
         }
         std::vector<int> victims;
         {
            std::lock_guard<std::mutex> lk(connMutex_);
            for(const auto&[fd, t] : lastActive_){
               if(pendingConn_.count(fd)) continue;
               if(!inFlight_.count(fd) && now-t > kIdleTimeout)
                  victims.push_back(fd);
            }  
         }
         for(int fd: victims) closeConnection(fd);
      }

      void HTTPServer::shutdownServer()
      {
         if(shutDown_)
         {
            return;
         }
         shutDown_ = true;

         threadPool.drain();

         std::vector<int> fds;
         {
            std::lock_guard<std::mutex> lk(connMutex_);
            fds.reserve(fdEpoll_.size());

            for(const auto& entry : fdEpoll_)  //选用fdepoll包括未发送请求和已发送请求的  若用lastActive则只有后者
            {
                  fds.push_back(entry.first);
            }
         }

         for(int fd : fds)
         {
            closeConnection(fd);
         }

         //  Redis 必须在这里关，而且要回到 #0 线程做：
         //    close() 会让未完成的 Redis 回调以 nullptr 跑一遍，那时 SubReactor 还活着才安全；
         //    晚一步（等到 ~HTTPServer）就是打在已销毁的 SubReactor 上。
         {
            if(!subReactors_.empty()){
               auto closed = std::make_shared<std::promise<void>>();   // 堆上持有：超时返回也不会悬垂
               auto fut = closed->get_future();
               subReactors_[0]->post([this, closed]{   // 值捕获 shared_ptr，不是引用
                  redis_.close();
                  closed->set_value();
               });
               fut.wait_for(std::chrono::milliseconds(500));   // 兜底超时：析构里还会再兜一次
            }
         }

         for(auto& sr : subReactors_)
         {
            sr->stop();
         }

         for(auto& sr : subReactors_)
         {
            sr->join();
         }

         subReactors_.clear();
      }


      //存活状态由sendResponse上传到send404与500  
      bool HTTPServer::send404(int clientSocket, bool keepAlive){ //发送错误码并返回链接存活状态
         std::string path =  dirRoot + "/404Response.html";
         std::string content = readHTMLFile(path); //无需关心m_time不需要传入第二个参数
         if(content.empty()){
            content =  "<html><body><h1>404 Not Found</h1></body></html>";
         }
         std::string response = createResponse(content, "text/html", 404, keepAlive);
         std::cerr << "Sending 404 Not Found response" << std::endl;
         return sendResponse(clientSocket, response); 
      }

      bool HTTPServer::send500(int clientSocket, bool keepAlive){
         std::string path = dirRoot + "/500Response.html";
         std::string content = readHTMLFile(path);
         if(content.empty()){
            content =  "<html><body><h1>500 Internal Server Error</h1></body></html>";
         }
         std::string response = createResponse(content, "text/html", 500, keepAlive);
         std::cerr << "Sending 500 error response" << std::endl;
         return sendResponse(clientSocket, response);
      }

       std::string HTTPServer::getMimeType(const std::string& path){
         size_t pos = path.find_last_of('.');
          if(pos == std::string::npos) return "application/octet-stream";
          std::string ext = path.substr(pos + 1);
          std::transform(ext.begin(), ext.end(), ext.begin(), [](unsigned char c){
            return std::tolower(c);
          });
          return mimeTypes.getType(ext);
          
          }

       std::string HTTPServer::readHTMLFile(const std::string& filename, struct timespec* outMtime){
         auto cached = fileCache.get(filename, outMtime);
         if(cached) {
            cacheHits_.fetch_add(1, std::memory_order_relaxed); //缓存命中计数加一
            return *cached;
         } //返回值是shared_ptr 即使被erase也不会死掉

         cacheMisses_.fetch_add(1, std::memory_order_relaxed);

         struct stat st;
         if(outMtime != nullptr && stat(filename.c_str(), &st) == 0){
            *outMtime = st.st_mtim;  //未命中 则需要重读硬盘最后修改时间并写给outMtime
         }

         std::ifstream file(filename);
         if(!file.is_open()){
            return "";
         }
            std::stringstream buffer;
            buffer <<file.rdbuf();
            fileCache.put(filename, buffer.str());
            return buffer.str();    
      }

      void HTTPServer::dispatchNewConnection(int clientFd)
   {
      if(subReactors_.empty())
      {
         close(clientFd);
         return;
      }

      size_t idx = nextSub_.fetch_add(1, std::memory_order_relaxed) % subReactors_.size();
      int targetEpfd = subReactors_[idx]->epollFd();

      if(!subReactors_[idx]->addConnection(clientFd))
      {
         close(clientFd);
         return;
      }

      {
         std::lock_guard<std::mutex> lk(connMutex_);
         fdEpoll_[clientFd] = targetEpfd;
         connGen_[clientFd] = ++genCounter_;
      }
   }



      void HTTPServer::dispatchReady(int fd, uint32_t events)
         {
            if(fd == redisFd_.load(std::memory_order_relaxed)){
               handleRedisEvent(events);
               return;
            }

            if(events & (EPOLLERR | EPOLLHUP))
            {
               closeConnection(fd);
               return;
            }

            if(events & EPOLLOUT)
            {
               threadPool.enqueue([this, fd]()
               {
                     handleWritable(fd);
               });
            }
            if(events & EPOLLIN)
            {
               threadPool.enqueue([this, fd]()
               {
                     handleOnce(fd);
               });
            }
         }

         void HTTPServer::handleRedisEvent(uint32_t events){
            if(!redis_.valid()){
               redisFd_.store(-1, std::memory_order_relaxed);
               return;
            }
            redis_.handleEvent(events);
         }
         

      bool HTTPServer::armOn(int epfd, int fd, uint32_t events)
         {
            epoll_event ev = makeEvent(fd, events);
            return epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev) == 0;
         }

      void HTTPServer::removeOn(int epfd, int fd)
         {
            epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
         }

      void HTTPServer::checkRatelimit(const std::string& ip, long long need,
                                std::function<void(bool, long long)> done){
         // 约定：只在 SubReactor #0 线程调用
         if(!done) return;          // 空 std::function 直接调用会抛 std::bad_function_call
         if(ratelimitSha_.empty() || !redis_.valid()){
            done(true, -1);              // 降级：Redis 不可用一律放行
            return;
         }

         long long now = static_cast<long long>(std::time(nullptr)); // 指针置空无需构造左值变量

         std::vector<std::string> keys{ "ratelimit:" + ip };
         std::vector<std::string> args{
            std::to_string(now),
            std::to_string(kRatelimitCapacity),
            std::to_string(kRatelimitRate),
            std::to_string(need)
         };

         redis_.evalBySha(ratelimitSha_, keys, args,
      [done = std::move(done)](redisReply* r){ //根据返回结果判断应该做什么事情
         if(!r || r->type != REDIS_REPLY_INTEGER){
            // r 出回调就被 free，错误文本必须当场打出来，否则 NOSCRIPT 之类的真因永远看不到
            if(r && r->type == REDIS_REPLY_ERROR){
               std::cerr << "限流脚本执行出错: " << r->str << std::endl;
            }
            done(true, -1);       // 出错一律放行，宁放过不误杀
            return;
         }
         long long remain = r->integer;
         done(remain >= 0, remain);
      });
      }


      std::string HTTPServer::peerIp(int fd){ 
         sockaddr_in addr{};
         socklen_t len = sizeof(addr);
         if(getpeername(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0){
            return "0.0.0.0";        // 拿不到就归到一个公共桶里，别返回空串
         }
         char buf[INET_ADDRSTRLEN]{};
         inet_ntop(AF_INET, &addr.sin_addr, buf, sizeof(buf));  //把网络二进制字节序转为可呈现地址
         return buf;
      }


      void HTTPServer::handleDownload(int fd, const std::string& userPath, bool keepAlive){
         // ── 段 A：worker 线程。只做「路径合不合法、文件在不在、多大」
         std::string rel = userPath.substr(std::string("/download/").size());

         // 不截的话 /download/a.apk?from=wx 会把 "?from=wx" 拼进磁盘路径 → 全站 404。
         size_t q = rel.find('?');
         if(q != std::string::npos) rel.resize(q);

         std::string diskPath = dirRoot + "/" + rel;

         //  路径穿越校验：realpath 会把 ".." 解开，所以必须先 realpath、再比前缀。
         //    只判字符串里有没有 ".." 是没用的（"....//" 这类变形能绕过）。
         char resolved[PATH_MAX];
         char rootResolved[PATH_MAX];
         if(realpath(diskPath.c_str(), resolved) == nullptr
            || realpath(dirRoot.c_str(), rootResolved) == nullptr){
            sendDownloadError(fd, 404, "404 Not Found", keepAlive); 
            return;
         }
         std::string realDisk  = resolved;
         std::string rootReal  = rootResolved;
         if(realDisk.compare(0, rootReal.size(), rootReal) != 0
            || (realDisk.size() > rootReal.size() && realDisk[rootReal.size()] != '/')){
            sendDownloadError(fd, 404, "404 Not Found", keepAlive); // 404：跑出 dirRoot 之外了
            return;
         }
         diskPath = realDisk;

         struct stat st{};
         if(stat(diskPath.c_str(), &st) != 0 || !S_ISREG(st.st_mode)){ //s_isreg  是否是普通文件
            sendDownloadError(fd, 404, "404 Not Found", keepAlive); 
            return;
         }

         // need 就是文件字节数，和 cap 同单位。用 stat，不查 MySQL。
         long long need = static_cast<long long>(st.st_size);
         std::string ip = peerIp(fd);

         // ── 段 B：占住连接 + 记世代号，然后跳到 #0
         uint64_t gen = 0;
         {
            std::lock_guard<std::mutex> lk(connMutex_);
            auto it = connGen_.find(fd);
            if(it == connGen_.end()){
               return;                      // 连接已经没了，什么都不用做
            }
            gen = it->second;
            pendingConn_.insert(fd);        // 告诉 sweep：这个 fd 在等东西，别杀
         }  

         subReactors_[0]->post([this, fd, gen, ip, diskPath, need, keepAlive]{ //第一层：从worker线程切换到sub#0 捕获需要的信息
         checkRatelimit(ip, need, [this, fd, gen, diskPath, keepAlive](bool ok, long long remain){ //第二层：sub#0向hiredis发送请求并登记回调函数
                                                                        // ok是是否放行 remain是剩余令牌桶容量
            threadPool.enqueue([this, fd, gen, diskPath, keepAlive, ok]{  //第三层：sub#0又把任务推给线程池 完成文件发送

               PendingGuard guard{this, fd, gen};  

               {
                  std::lock_guard<std::mutex> lk(connMutex_);
                  auto it = connGen_.find(fd);
                  if(it == connGen_.end() || it->second != gen){
                     return;             
                  }
               }

               if(!ok){  
                  sendDownloadError(fd, 429, "429 Too Many Requests", keepAlive);
                  return;  
               }
               sendDownloadFile(fd, gen, diskPath, keepAlive);
            });
         });
      });
   }


   void HTTPServer::sendDownloadFile(int fd, uint64_t gen, const std::string& diskPath,
                                  bool keepAlive){
         // 先把文件拿到手 —— 这一步只碰磁盘，绝对不碰客户端 fd
         //  O_NONBLOCK 只为一件事：万一路径变成了 FIFO，open 会阻塞到有写者 ， 那会挂死一个 worker。
         //  普通文件忽略这个标志，后面的 sendfile 照样跑（非阻塞性看的是 socket 那一侧）。
         int rawFd = ::open(diskPath.c_str(), O_RDONLY | O_CLOEXEC | O_NONBLOCK);//只读打开， exec时自动关闭文件fd，防止卡住
         auto file = (rawFd >= 0) ? std::make_shared<FileFdOwner>(rawFd)
                                 : nullptr;               // RAII：任何 return 都会 close

         // 长度和类型都从手上这个 fd问
         struct stat fst{};
         bool ready = (rawFd >= 0
                     && ::fstat(rawFd, &fst) == 0
                     && S_ISREG(fst.st_mode));
         long long fileSize = ready ? static_cast<long long>(fst.st_size) : 0;

         //  身份校验必须在「发任何一个字节」之前，包括那句 404。
         //   排队等 worker 期间连接可能已经断了、fd 号被新连接复用；对着旧号码 send 一句 404，
         //  就是往别人的响应流里插一段垃圾。所以顺序是：先 open/fstat（纯磁盘）→ 再校验 → 最后才写字。
         {
            std::lock_guard<std::mutex> lk(connMutex_);
            auto it = connGen_.find(fd);
            if(it == connGen_.end() || it->second != gen){
               return;                          // 连接已死：不发、也不登记（file 析构自动 close）
            }
            if(!keepAlive && ready){
               closeAfterWrite_.insert(fd);     // 只给「真发文件」的情况打标记
            }
         }

         // 文件不可用 → 404（此时已经确认连接还是我们的）
         if(!ready){
            sendDownloadError(fd, 404, "404 Not Found", keepAlive);   // file 析构 → close
            return;
         }

         // ③ 只拼响应头。Content-Type 按后缀走，apk → application/vnd.android.package-archive
         //    mtime 用 fstat 拿到的 st_mtim，客户端下次带 If-Modified-Since 就能判 304（板块 4）
         std::string header = createDownloadHeader(getMimeType(diskPath), fileSize, 200, keepAlive,
                                                   &fst.st_mtim);

         const char* data = header.data();
         size_t toSend    = header.size();
         if(!trySend(fd, data, toSend)){
            closeConnection(fd);
            return;
         }

         
         //   必须先发完头才能 sendfile，否则客户端收到的字节顺序是乱的。
         if(toSend > 0){// 头没一次发完 → 剩余塞进 outBuffers_，并登记outBuffers_
            {
               std::lock_guard<std::mutex> lk(connMutex_);
               outBuffers_[fd].append(data, toSend);
               if(fileSize > 0){
                  SendFileState st;
                  st.file      = std::move(file);
                  st.offset    = 0;
                  st.remaining = static_cast<size_t>(fileSize);
                  sendFileStates_[fd] = std::move(st);  //登记需要发送的文件， 在handlewritable里检查， 发送完头后发送主体
               }
            }
            finishOnce(fd);            // 直接走 arm epollout 等头发送完之后 再发送剩余文件
            return;
         }

         if(fileSize == 0){ //空文件直接收尾
            
            bool closing = false; //是否需要关闭
            {
               std::lock_guard<std::mutex> lk(connMutex_);
               auto it = closeAfterWrite_.find(fd);
               if(it != closeAfterWrite_.end()){ closeAfterWrite_.erase(it); closing = true; }
            }
            if(closing) closeConnection(fd);
            else        finishOnce(fd);
            return;
         }

         //  登记状态，开始发文件体
         {
            std::lock_guard<std::mutex> lk(connMutex_);
            SendFileState st;   //发送状态结构体
            st.file      = std::move(file);  //指向filefdowner的shared_ptr
            st.offset    = 0;
            st.remaining = static_cast<size_t>(fileSize);
            sendFileStates_[fd] = std::move(st);
         }
         sendFileBody(fd);


      }


      void HTTPServer::sendFileBody(int fd){
         std::shared_ptr<FileFdOwner> file;
         off_t  offset    = 0;
         size_t remaining = 0;
         {
            std::lock_guard<std::mutex> lk(connMutex_);
            auto it = sendFileStates_.find(fd);
            if(it == sendFileStates_.end()){
               finishOnce(fd);                       // 没有在途的 sendfile，交回普通收尾
               return;
            }
            file      = it->second.file;             // 拷一份 shared_ptr，锁外期间保活
            offset    = it->second.offset;
            remaining = it->second.remaining;
         }

         ssize_t n;
         do{
            n = ::sendfile(fd, file->fd, &offset, remaining);
         }while(n < 0 && errno == EINTR);            // 被信号打断就重试

         if(n < 0){
            if(errno == EAGAIN || errno == EWOULDBLOCK){
               // 内核发送缓冲区满 —— 完全正常，记住进度等 EPOLLOUT
               saveSendFileProgress(fd, offset, remaining);
               finishOnce(fd);
               return;
            }
            closeConnection(fd);             //连接已断
            return;
         }
         if(n == 0){  //正在下载时候 文件被改小了 比如同名覆盖
            closeConnection(fd);
            return;
         }

          remaining -= static_cast<size_t>(n);

         if(remaining == 0){
            // 发完了：先摘表（FileFdOwner 析构 → close(fileFd)），再看要不要关连接
            bool closing = false;
            {
               std::lock_guard<std::mutex> lk(connMutex_);
               sendFileStates_.erase(fd);
               auto it = closeAfterWrite_.find(fd);
               if(it != closeAfterWrite_.end()){ closeAfterWrite_.erase(it); closing = true; }
            }
            if(closing) closeConnection(fd);
            else        finishOnce(fd);
            return;
         }

         saveSendFileProgress(fd, offset, remaining);
         finishOnce(fd);                             // 还有剩 → arm EPOLLOUT
      }

      void HTTPServer::saveSendFileProgress(int fd, off_t offset, size_t remaining){ //没写完的存回去记录
         std::lock_guard<std::mutex> lk(connMutex_);
         auto it = sendFileStates_.find(fd);
         if(it != sendFileStates_.end()){
            it->second.offset    = offset;
            it->second.remaining = remaining;
         }
      }
               



    HTTPServer::HTTPServer(int port) : 
               port(port), 
               running(false), 
               initialized(false),
               threadPool(std::max<size_t> (2, std::thread::hardware_concurrency())) {
                  if(!mimeTypes.loadFromFile("mime.types")){
                     mimeTypes.setDefaults();
                  }

                  char resolved[PATH_MAX];
                  if(realpath(dirRoot.c_str(), resolved) == nullptr){
                     std::cerr<<"文件根目录解析失败："<<dirRoot<<std::endl;
                  }
                  else{
                     dirRoot = resolved;
                     initialized = true;
                  }
               }

                   



    bool HTTPServer::start(){
      for(size_t i = 0; i < kSubReactorCount; ++i)
      {
         auto sr = std::make_unique<SubReactor>();

         bool reactorOk = sr->start([this](int fd, uint32_t ev)
         {
               dispatchReady(fd, ev);
         });

         if(!reactorOk)
         {
               std::cerr << "SubReactor " << i << " 启动失败" << std::endl;
               return false;
         }

         subReactors_.push_back(std::move(sr));
      }

      bool initOk = epollEngine.init(port, 128, [this](int clientFd)
      {
         dispatchNewConnection(clientFd);
      });

      if(!initOk || !initialized)
      {
         std::cerr << "服务器初始化失败" << std::endl;
         return false;
      }

      running = true;
      std::cout << "服务器启动成功!" << std::endl;
      std::cout << "访问地址:http://localhost:" << port << std::endl;
      std::cout << "按ctrl+c停止服务器" << std::endl;

         subReactors_[0]->post([this]{

      SubReactor* sub = subReactors_[0].get();

      //  第 4 个参数是断连通知，被动断连时靠它把这边两个状态一起清掉。
      //    redisFd_ 留着旧值 → 号被新 HTTP 连接复用 → dispatchReady 误判成 Redis 事件
      //    ratelimitSha_ 留着旧值 → Redis 重启后缓存已空，每次 EVALSHA 收 NOSCRIPT
      if(!redis_.connect("127.0.0.1", 6379, sub, [this]{
            redisFd_.store(-1, std::memory_order_relaxed);
            ratelimitSha_.clear();
         })){
         std::cerr << "Redis 连接失败，限流功能降级" << std::endl;
         return;                    // 不退出：静态页面不需要 Redis
      }
      redisFd_.store(redis_.fd(), std::memory_order_relaxed);

      redis_.scriptLoad(kRatelimitScript, [this](redisReply* r){
         if(r && r->type == REDIS_REPLY_STRING){
            ratelimitSha_.assign(r->str, r->len);
            std::cout << "限流脚本已加载 sha=" << ratelimitSha_ << std::endl;
         }else{
            // r->str 里有 Lua 编译错误的行号和原因，出回调就没了，必须当场打
            if(r && r->type == REDIS_REPLY_ERROR){
               std::cerr << "限流脚本 LOAD 失败: " << r->str << std::endl;
            }else{
               std::cerr << "限流脚本 LOAD 失败（无回复，Redis 可能未连接）" << std::endl;
            }
            std::cerr << "限流降级：所有下载请求一律放行" << std::endl;
         }
      });
   });

      bool ok = epollEngine.run([this](int clientSocket, uint32_t events)
      {
         dispatchReady(clientSocket, events);
      }, [this]()
      {
         sweepIdleConnection();
      });

      shutdownServer();
      return ok;
   }



    void HTTPServer::stop(){
      running = false;
      epollEngine.stop();
    }

    HTTPServer::~HTTPServer() {
      stop();
    }

