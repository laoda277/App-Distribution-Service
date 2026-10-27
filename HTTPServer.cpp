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
#include <future>
#include <memory>
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
         {
            std::lock_guard<std::mutex> lk(connMutex_);
            lastActive_[fd] = std::chrono::steady_clock::now();
            inFlight_.insert(fd);
            auto it = outBuffers_.find(fd);
            if(it != outBuffers_.end()){
               out = std::move(it -> second);
               outBuffers_.erase(it);
            }
         }

         const char* data = out.data();
         size_t toSend = out.size();
         if(!trySend(fd, data, toSend)){
            closeConnection(fd);
            return;
         }
         if(toSend > 0){
            std::lock_guard<std::mutex> lk(connMutex_);
            outBuffers_[fd].append(data, toSend);
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

