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
#include <sys/ioctl.h>          //FIONREAD：关连接前查接收缓冲里还剩多少没读的字节
#include<arpa/inet.h>
#include <cstdio>       // snprintf / sscanf
#include<netinet/in.h>
#include "nlohmann/json.hpp" 
#include "HTTPServer.h"
#include "Orm.h"


   static epoll_event makeEvent(int fd, uint32_t events){
    epoll_event ev{};
    ev.events = events | EPOLLONESHOT;
    ev.data.fd = fd;
    return ev;
   }

   static std::string makeSortKey(int a, int b, int c){  //统一版本号字符串 方便排序 但只保证最少3位 
      char buf[16];
      std::snprintf(buf, sizeof(buf), "%03d.%03d.%03d", a, b, c);
      return std::string(buf);                  // 1.10.2 → "001.010.002"
   }

   static int hexVal(char c){
      if(c >= '0' && c <= '9') return c - '0';
      if(c >= 'a' && c <= 'f') return c - 'a' + 10;
      if(c >= 'A' && c <= 'F') return c - 'A' + 10;
      return -1;                       // 非法字符用 -1 表示
   }

   static bool urlDecodePath(const std::string& in, std::string& out){ //path把%xx转为对应字符 +不转为空格
      out.clear();
      for(size_t i = 0; i < in.size(); ++i){
         if(in[i] != '%'){
            out += in[i];
            continue;
         }
         // 尾巴上孤零零一个 %：后面不够两个字符
         if(i + 2 >= in.size()){ return false; }
         int hi = hexVal(in[i + 1]), lo = hexVal(in[i + 2]);
         if(hi < 0 || lo < 0){ return false; }   // %GG 之类
         out += static_cast<char>(hi * 16 + lo);
         i += 2;
      }
      return true;
   }

   static bool urlDecodeQuery(const std::string& in, std::string& out){ //把%xx转为对应字符，+转为空格
      out.clear();
      for(size_t i = 0; i < in.size(); ++i){
         if(in[i] == '+'){
            out += ' ';
            continue;
         }
         if(in[i] != '%'){
            out += in[i];
            continue;
         }
         if(i + 2 >= in.size()) return false;          // 尾巴上孤零零一个 %
         int hi = hexVal(in[i + 1]);
         int lo = hexVal(in[i + 2]);
         if(hi < 0 || lo < 0) return false;            // %GG 之类
         out += static_cast<char>(hi * 16 + lo);
         i += 2;
      }
      return true;
   }

   static bool queryGet(const std::string& query, const std::string& key,
                     std::string& out){                              //把query里key对应的value提取出来
      size_t pos = 0;
      while(pos <= query.size()){
         size_t amp = query.find('&', pos);
         if(amp == std::string::npos) amp = query.size();
         const std::string pair = query.substr(pos, amp - pos);
         const size_t eq = pair.find('=');
         const std::string k = (eq == std::string::npos) ? pair : pair.substr(0, eq);
         if(k == key){
            out = (eq == std::string::npos) ? std::string() : pair.substr(eq + 1);
            return true;
         }
         if(amp == query.size()) break;
         pos = amp + 1;
      }
      return false;
   }


      

   static constexpr long long kRatelimitBurst = 1000000;  // 桶容量 20次  [下载]
   static constexpr long long kRatelimitQps = 1000000;  // 每秒补 10次   [下载]

   // /pkg/ 不吐字节，只查一次库就回 302。它要防的不是带宽而是「把 MySQL 刷穿」，
   // 所以配额放宽一个数量级，并且用独立 key —— 两条路径的桶互不占用。
   static constexpr long long kPkgRatelimitBurst = 100;  // 桶容量 100次
   static constexpr long long kPkgRatelimitQps   = 50;   // 每秒补 50次
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
       const size_t kMaxHeader = 8192;            // 请求行 + 头 的上限（原来的 kMaxTotal，语义收窄）
       const size_t kMaxBody   = 64 * 1024;       // 非上传请求的 body 上限，本项目的 JSON 只有几百字节
       char chunk[4096];
       bool sentContinue = false;                 // Expect: 100-continue 只回一次

       while(true){
         size_t pos = connBuffer.find("\r\n\r\n");
         if(pos != std::string::npos){
            const size_t headLen = pos + 4;
            const std::string head = connBuffer.substr(0, headLen);

            //  上传请求的 body 可能是 GB 级，必须由 beginRecv 从 connBuffer 里流式吃
            //    （beginRecv 里 "buf 里已经躺着 body 的开头" 那段就是干这个的）。
            //    所以这里一旦预读，body 就被吞进 request，beginRecv 拿到的 connBuffer 是空的
            if(isUploadRequest(head)){
               request = connBuffer.substr(0, headLen);
               connBuffer.erase(0, headLen);
               return ReadStatus::Request;
            }

            // 非上传请求 判定消息体长度
            // 看到 Transfer-Encoding 就拒绝。本项目不支持 chunked，绝不按 Content-Length 去读，
            if(!extractHeader(head, "transfer-encoding").empty()) return ReadStatus::BadRequest;

            long long bodyLen = 0;
            if(!extractHeader(head, "content-length").empty()){
               bodyLen = parseContentLength(head);       
               if(bodyLen < 0) return ReadStatus::BadRequest;
            }
            if(bodyLen > static_cast<long long>(kMaxBody)) return ReadStatus::PayloadTooLarge;

            const size_t need = headLen + static_cast<size_t>(bodyLen);
            if(connBuffer.size() >= need){
               request = connBuffer.substr(0, need);
               connBuffer.erase(0, need);
               return ReadStatus::Request;
            }

            // 头齐了但 body 没齐。客户端可能带着 Expect: 100-continue 在等我们点头，
            // 不答它就只能干等到自己的超时（curl 会白等 1 秒）。
            if(!sentContinue && extractHeader(head, "expect").find("100-continue") != std::string::npos){
               static const char kContinue[] = "HTTP/1.1 100 Continue\r\n\r\n";
               ::send(clientSocket, kContinue, sizeof(kContinue) - 1, MSG_NOSIGNAL);
               sentContinue = true;
            }
         }
         else{
            // 头还没齐，这时才用 header 上限判断
            if(connBuffer.size() >= kMaxHeader) return ReadStatus::TooLarge;
         }

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

   bool HTTPServer::methodIs(const std::string& request, const std::string& method){
      // 请求行形如 "POST /api/releases HTTP/1.1"，动词就是第一个空格之前那一段。
      // HTTP 方法名大小写敏感（RFC 9110），不做 tolower。
      if(request.size() <= method.size()) return false;
      if(request.compare(0, method.size(), method) != 0) return false;
      // 必须紧跟空格，否则 "POSTER" 会被误判成 "POST"
      return request[method.size()] == ' ';
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
       else if(statusCode==400){
          statusMessage="Bad Request";
       }
       else if(statusCode==403){
          statusMessage="Forbidden";
       }
       else if(statusCode==411){
          statusMessage="Length Required";
       }
       else if(statusCode==413){
          statusMessage="Payload Too Large";
       }
       else if(statusCode==431){
          statusMessage="Request Header Fields Too Large";
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
                                             const struct timespec* mtime,
                                             long long rangeStart, long long rangeEnd, long long totalSize){   // 默认值只写在 .h
         const char* statusMessage = "Unknown";
         switch(statusCode){
            case 200: statusMessage = "OK";                    break;
            case 206: statusMessage = "Partial Content";        break;
            case 400: statusMessage = "Bad Request";           break;
            case 403: statusMessage = "Forbidden";             break;
            case 404: statusMessage = "Not Found";             break;
            case 411: statusMessage = "Length Required";       break;
            case 413: statusMessage = "Payload Too Large";     break;
            case 416: statusMessage = "Range Not Satisfiable";  break;
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
         response << "Accept-Ranges: bytes\r\n"; //声明有能力
         if(statusCode == 416){
            // 416 只报总长，不给区间 —— 语法就是这么规定的：bytes */<total>
            response << "Content-Range: bytes */" << totalSize << "\r\n";
         }else if(statusCode == 206){
            response << "Content-Range: bytes " << rangeStart << "-"  //声明本次区间
                     << rangeEnd << "/" << totalSize << "\r\n";
         }
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
         if(!keepAlive){
            // Connection: close 只是响应头里的一句话，服务端不会因此自动关连接。
            // 真正关要打 closeAfterWrite_ 标记，handleWritable 把响应发完之后才 close。
            std::lock_guard<std::mutex> lk(connMutex_);
            closeAfterWrite_.insert(fd);
         }
         finishOnce(fd);     // finishOnce 会自动 arm EPOLLOUT
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
         std::string query;
         const size_t qpos = userPath.find('?');  //提取纯净路径 删除参数
         if(qpos != std::string::npos){
            query = userPath.substr(qpos + 1);
            userPath.erase(qpos);
         }
         totalRequests_.fetch_add(1, std::memory_order_relaxed); //最宽松约束 无需关注顺序

         if(userPath == "/" || userPath .empty()){
            userPath = "/index.html";
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

            nlohmann::ordered_json out;
            out["uptime_seconds"] = uptime;
            out["total_requests"] = totalRequests_.load(std::memory_order_relaxed);
            out["cache_hits"]     = cacheHits_.load(std::memory_order_relaxed);
            out["cache_misses"]   = cacheMisses_.load(std::memory_order_relaxed);
            out["in_flight"]      = inFlight;
            out["log_pending"]    = logWriter_.pending();   // 队列积压
            out["log_dropped"]    = logWriter_.dropped();   // 队列满丢弃累计
            out["log_pruned"]     = logWriter_.pruned();    // 保留策略已删除累计
            out["connections_per_reactor"] = nlohmann::ordered_json::array();
            for(const auto& r : subReactors_){
               out["connections_per_reactor"].push_back(r->connectionCount());
            }

            std::string response = createResponse(out.dump(), "application/json", 200, keep);
         bool alive = sendResponse(clientSocket, response);
         return keep && alive;
            
         }
         if(userPath.compare(0, 5, "/pkg/") == 0){
            handlePkg(clientSocket, userPath, query, keep);
            return true;
         }
         if(userPath.compare(0, 10, "/download/") == 0){
            handleDownload(clientSocket, userPath, keep,  extractHeader(request, "range"));
            return true;  //这里不管 关不关闭由closeAfterWrite_ 决定
         }

         if(userPath == "/api/releases" && methodIs(request, "POST")){  //上传文件版本映射
            bool alive = handleCreateRelease(clientSocket, request, keep);
            return keep && alive;   // 同步处理，成败自己知道
         }
         if(userPath == "/api/releases" && methodIs(request, "GET")){   //读文件版本映射
            bool alive = handleListReleases(clientSocket, query, keep);
            return keep && alive;
         }
         if(userPath == "/api/products" && methodIs(request, "GET")){   //产品列表（按产品聚合版本数）
            bool alive = handleListProducts(clientSocket, query, keep);
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
         bool receiving = false;
          {
            std::lock_guard<std::mutex> lk(connMutex_);
            receiving = (recvFileStates_.count(clientSocket) > 0);  //对于数据到达的链接来说 假如是正在接受文件的链接（不是新链接，不用处理请求头） 就走continueRecv分支
          }
            if(receiving){ continueRecv(clientSocket); return; }


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

               if(isUploadRequest(request)){  //上传文件的路径
                  if(!beginRecv(clientSocket, request, buf)){
                     return;                    // beginRecv 内部已经决定了 close 还是 arm
                  }
                  bool stillReceiving = false;
                  {
                     std::lock_guard<std::mutex> lk(connMutex_);
                     stillReceiving = (recvFileStates_.count(clientSocket) > 0);
                  }
                  if(stillReceiving) return;    // 还没传完，已 arm EPOLLIN，剩下的也是 body
                  continue;                     // 小文件当场完成，接着处理下一个请求
               }

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

            // 协议层错误：按原因回对应状态码
            int code = 0;
            const char* title = nullptr;
            switch(st){
               case ReadStatus::TooLarge:        code = 431; title = "431 Request Header Fields Too Large"; break;
               case ReadStatus::BadRequest:      code = 400; title = "400 Bad Request";                break;
               case ReadStatus::PayloadTooLarge: code = 413; title = "413 Payload Too Large";          break;
               default: break;   // Closed / Error：连接已经废了，不发响应
            }
            if(code != 0){
               // 不用 sendDownloadError：它内部发送失败时会自己 closeConnection，
               // 外面再 close 一次就是重复 close，而 fd 号此时可能已被新连接复用 → 误杀别人。
               std::string body = std::string("<html><body><h1>") + title + "</h1></body></html>";
               if(sendResponse(clientSocket, createResponse(body, "text/html", code, false))){
                  // 错误响应不足 200 字节，正常情况下一次 send 就发完了。
                  // 万一对端窗口为 0 没发完，就交给 handleWritable 发完再关，避免只发半截。
                  bool pending = false;
                  {
                     std::lock_guard<std::mutex> lk(connMutex_);
                     auto it = outBuffers_.find(clientSocket);
                     pending = (it != outBuffers_.end() && !it->second.empty());
                     if(pending) closeAfterWrite_.insert(clientSocket);
                  }
                  if(pending){
                     finishOnce(clientSocket);   // 会 arm EPOLLOUT
                     return;
                  }
               }
            }
            closeConnection(clientSocket);   // 内部带 lingering，先把接收缓冲读干净再 close
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

               auto sendfileIt = sendFileStates_.find(fd);                                      //没有被限流 可以epollout
               if(sendfileIt != sendFileStates_.end() && sendfileIt ->second.remaining > 0 && !sendfileIt->second.throttled)
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

   
         // 标准做法两步：① shutdown 写端发 FIN，让对端知道我们不发了
         //              ② 把接收方向读干净丢弃，再 close
         {
            int pending = 0;
            if(::ioctl(fd, FIONREAD, &pending) == 0 && pending > 0){
               ::shutdown(fd, SHUT_WR);
               char sink[4096];
               // 最多丢 256KB。恶意客户端如果一直灌，丢掉上限后就不再等了——
               // 这里是同步阻塞的，不设上限会让一个 worker 被拖住。
               // 真正的异步 lingering 要留 EPOLLIN 读到 EOF，本项目按 1 秒 sweep 的粒度做不到
               // 那么细，取「有界同步 drain」这个折中。
               for(int i = 0; i < 64; ++i){
                  ssize_t n = ::recv(fd, sink, sizeof(sink), 0);
                  if(n > 0) continue;
                  if(n < 0 && errno == EINTR) continue;
                  break;              // 0 = 对端已关；EAGAIN = 当前读干净了
               }
            }
         }

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
            // 必须交给归属的 SubReactor 去摘：只有它的 removeConnection 会在 DEL 成功后
            // 把 connCount_ 减一。直接 removeOn 只是裸 epoll_ctl(DEL)，计数永远只增不减，
            // /api/status 里的 connections_per_reactor 就退化成了「历史累计连接数」。
            SubReactor* owner = nullptr;
            for(auto& r : subReactors_){
               if(r->epollFd() == epfd){ owner = r.get(); break; }
            }
            if(owner) owner->removeConnection(fd);
            else      removeOn(epfd, fd);       // 不在任何 sub 上（主 reactor 自己的 fd）
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
            sendBuckets_.erase(fd);        // 桶跟着连接一起销毁
            recvFileStates_.erase(fd); 
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

         std::vector<std::pair<int, uint64_t>> resume;  //判断逻辑:收集所有被限流的连接，交给线程池去除限流标志然后用finishOnce重新epollout
         {
            std::lock_guard<std::mutex> lk(connMutex_);
            for(const auto& [fd, st] : sendFileStates_){
               if(!st.throttled) continue;
               auto git = connGen_.find(fd);
               if(git == connGen_.end()) continue;
               resume.emplace_back(fd, git->second);
            }
         }
         for(const auto& [fd, gen] : resume){
            threadPool.enqueue([this, fd, gen]{
               {
                  std::lock_guard<std::mutex> lk(connMutex_);
                  auto it = connGen_.find(fd);
                  if(it == connGen_.end() || it->second != gen) return;  // fd 已被复用
                  auto sit = sendFileStates_.find(fd);
                  if(sit == sendFileStates_.end()) return;
                  sit->second.throttled = false;
               }
               finishOnce(fd);   //内部会区分限流标志 不限流了就装epollout
            });
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

      void HTTPServer::checkRatelimit(const std::string& key, long long need,
                                long long capacity, long long rate,
                                std::function<void(bool, long long)> done){
         // 约定：只在 SubReactor #0 线程调用
         if(!done) return;          // 空 std::function 直接调用会抛 std::bad_function_call
         if(ratelimitSha_.empty() || !redis_.valid()){
            done(true, -1);              // 降级：Redis 不可用一律放行
            return;
         }

         long long now = static_cast<long long>(std::time(nullptr)); // 指针置空无需构造左值变量

         std::vector<std::string> keys{ key };
         std::vector<std::string> args{
            std::to_string(now),
            std::to_string(capacity),
            std::to_string(rate),
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


      void HTTPServer::handleDownload(int fd, const std::string& userPath, bool keepAlive, const std::string& rangeHeader){
         // ── 段 A：worker 线程。只做「路径合不合法、文件在不在、多大」
         std::string rel = userPath.substr(std::string("/download/").size());

         // 不截的话 /download/a.apk?from=wx 会把 "?from=wx" 拼进磁盘路径 → 全站 404。
         size_t q = rel.find('?');
         if(q != std::string::npos) rel.resize(q);

         std::string diskPath = dataRoot + "/" + rel;

         //  路径穿越校验：realpath 会把 ".." 解开，所以必须先 realpath、再比前缀。
         //    只判字符串里有没有 ".." 是没用的（"....//" 这类变形能绕过）。
         char resolved[PATH_MAX];
         char rootResolved[PATH_MAX];
         if(realpath(diskPath.c_str(), resolved) == nullptr
            || realpath(dataRoot.c_str(), rootResolved) == nullptr){
            sendDownloadError(fd, 404, "404 Not Found", keepAlive); 
            return;
         }
         std::string realDisk  = resolved;
         std::string rootReal  = rootResolved;
         if(realDisk.compare(0, rootReal.size(), rootReal) != 0
            || (realDisk.size() > rootReal.size() && realDisk[rootReal.size()] != '/')){
            sendDownloadError(fd, 404, "404 Not Found", keepAlive); // 404：跑出 dataRoot 之外了
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

         ByteRange range;                              // 默认 partial=false，整份发
         RangeParse rp = RangeParse::None;

         if(!rangeHeader.empty()){
            rp = parseRangeSpec(rangeHeader, need, range);
         }

         // 416：语法认得、但起点越界。只回头，不给正文。
         if(rp == RangeParse::Unsatisfiable){
            // 不能走 sendDownloadError —— 它不会带 Content-Range: bytes */N，
            //    客户端拿不到总长就无法自我修正，只能整个放弃。
            std::string header = createDownloadHeader(getMimeType(diskPath), 0, 416, keepAlive,
                                                      nullptr, 0, 0, need);
            if(!sendResponse(fd, header)){
               closeConnection(fd);
               return;
            }
            finishOnce(fd);
            return;
         }

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

         subReactors_[0]->post([this, fd, gen, ip, diskPath, need, keepAlive, range]{ //第一层：从worker线程切换到sub#0 捕获需要的信息
            long long billed = range.partial ? (range.end - range.start +1) : need;
         checkRatelimit("ratelimit:dl:" + ip, 1, kRatelimitBurst, kRatelimitQps, [this, fd, gen, ip, diskPath, keepAlive, range, billed](bool ok, long long remain){ //第二层：sub#0向hiredis发送请求并登记回调函数
                                                                        // ok是是否放行 remain是剩余令牌桶容量
            threadPool.enqueue([this, fd, gen, ip, diskPath, keepAlive, ok, range, billed]{  //第三层：sub#0又把任务推给线程池 完成文件发送

               PendingGuard guard{this, fd, gen};  

               {
                  std::lock_guard<std::mutex> lk(connMutex_);
                  auto it = connGen_.find(fd);
                  if(it == connGen_.end() || it->second != gen){
                     return;             
                  }
               }

               if(!ok){  
                  logWriter_.write(shaFromPath(diskPath), ip, 0, 429);  //限流也需要写入日志
                  sendDownloadError(fd, 429, "429 Too Many Requests", keepAlive);
                  return;  
               }
               logWriter_.write(shaFromPath(diskPath), ip, billed, 200);
               sendDownloadFile(fd, gen, diskPath, keepAlive, range);
            });
         });
      });
   }


   void HTTPServer::sendDownloadFile(int fd, uint64_t gen, const std::string& diskPath,
                                  bool keepAlive, const ByteRange& range){
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

         const long long start   = range.partial ? range.start : 0;
         const long long end     = range.partial ? std::min(range.end, fileSize - 1)
                                                : fileSize - 1;
         const long long sendLen = end - start + 1;        // fileSize==0 时正好是 0
         const int       status  = range.partial ? 206 : 200;

         std::string header = createDownloadHeader(getMimeType(diskPath), sendLen, status, keepAlive,
                                                   &fst.st_mtim, start, end, fileSize);

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
               if(sendLen > 0){
                  SendFileState st;
                  st.file      = std::move(file);
                  st.offset    = static_cast<off_t>(start);
                  st.remaining = static_cast<size_t>(sendLen);
                  sendFileStates_[fd] = std::move(st);  //登记需要发送的文件， 在handlewritable里检查， 发送完头后发送主体
               }
            }
            finishOnce(fd);            // 直接走 arm epollout 等头发送完之后 再发送剩余文件
            return;
         }

         if(sendLen == 0){ //空文件直接收尾
            
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
            st.offset    = start;
            st.remaining = static_cast<size_t>(sendLen);
            sendFileStates_[fd] = std::move(st);
         }
         sendFileBody(fd);

      }


      void HTTPServer::sendFileBody(int fd){
         std::shared_ptr<FileFdOwner> file;
         off_t  offset    = 0;
         size_t remaining = 0;
         size_t tokens = 0;
         bool gone = false;
         {
            std::lock_guard<std::mutex> lk(connMutex_);
            auto it = sendFileStates_.find(fd);
            if(it == sendFileStates_.end()){
               gone = true;                 // 连接已经没了，锁外 finishOnce 后直接返回
            }else{
               file      = it->second.file;             // 拷一份 shared_ptr，锁外期间保活
               offset    = it->second.offset;
               remaining = it->second.remaining;

               // 桶跟着 fd 走；operator[] 首次自动构造为满桶
               // 只在连接还活着时碰 sendBuckets_，否则会给死 fd 留一条永不回收的桶
               SendBucket& bucket = sendBuckets_[fd];
               auto now = std::chrono::steady_clock::now();
               auto us  = std::chrono::duration_cast<std::chrono::microseconds>(
                           now - bucket.lastRefill).count();
               long long add = us * static_cast<long long>(kSendRateBytesPerSec) / 1000000;
               if(add > 0){
                  bucket.tokens = std::min<long long>(
                     static_cast<long long>(kSendBurstBytes),   //读取还能再发送多少字节
                     static_cast<long long>(bucket.tokens) + add);
                  bucket.lastRefill = now;
               }
               tokens = bucket.tokens;
            }
         }

         if(gone){
            finishOnce(fd);
            return ;
         }

            // 额度为 0：不 arm EPOLLOUT，交给 sweep 唤醒（否则忙等打满 CPU）
         if(tokens == 0){
            saveSendFileProgress(fd, offset, remaining, 0, true);
            finishOnce(fd);          // throttled → 内部 arm EPOLLIN
            return;
         }

         const size_t toSend = std::min(remaining, tokens);
         ssize_t n;
         do{
            n = ::sendfile(fd, file->fd, &offset, toSend);
         }while(n < 0 && errno == EINTR);            // 被信号打断就重试

         if(n < 0){
            if(errno == EAGAIN || errno == EWOULDBLOCK){
               // 内核发送缓冲区满 —— 完全正常，记住进度等 EPOLLOUT
               saveSendFileProgress(fd, offset, remaining, tokens, false);
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
          tokens    -= static_cast<size_t>(n);

         if(remaining == 0){
            // 发完了：先摘表（FileFdOwner 析构 → close(fileFd)），再看要不要关连接
            bool closing = false;
            {
               std::lock_guard<std::mutex> lk(connMutex_);
               auto bt = sendBuckets_.find(fd);
               if(bt != sendBuckets_.end()) bt->second.tokens = tokens;
               sendFileStates_.erase(fd);
               auto it = closeAfterWrite_.find(fd);
               if(it != closeAfterWrite_.end()){ closeAfterWrite_.erase(it); closing = true; }
            }
            if(closing) closeConnection(fd);
            else        finishOnce(fd);
            return;
         }
            // 额度用完但文件没发完 → 标记暂停，sweep 到点唤醒
         if(tokens == 0){
            saveSendFileProgress(fd, offset, remaining, 0, true);
            finishOnce(fd);
            return;
         }

         saveSendFileProgress(fd, offset, remaining, tokens, false);
         finishOnce(fd);                             // 还有剩 → arm EPOLLOUT
      }

      void HTTPServer::saveSendFileProgress(int fd, off_t offset, size_t remaining, size_t tokens, bool throttled){ //没写完的存回去记录
         std::lock_guard<std::mutex> lk(connMutex_);
         auto it = sendFileStates_.find(fd);
         if(it == sendFileStates_.end()) return;
         it->second.offset    = offset;
         it->second.remaining = remaining;
         it->second.throttled = throttled;

         auto bt = sendBuckets_.find(fd);
         if(bt != sendBuckets_.end()) bt->second.tokens = tokens;
      }

      bool HTTPServer::writeAllToDisk(int fd, const char* data, size_t len){  //把数据写入磁盘
      // 这里只是把数据交给内核 page cache，不是真的落盘
         while(len > 0){
            ssize_t n = ::write(fd, data, len);
            if(n < 0){
               if(errno == EINTR) continue;
               return false;
            }
            if(n == 0) return false;   // 一个字节都没写进去，再循环就是死循环
            data += n;
            len  -= static_cast<size_t>(n);
         }
         return true;
      }

      std::string HTTPServer::sanitizeFileName(const std::string& raw){  //把文件名清洗为安全的形式
         if(raw.empty() || raw.size() > 200) return "";
         std::string out;
         out.reserve(raw.size());
         for(char c : raw){
            unsigned char u = static_cast<unsigned char>(c);
            // 只放行：字母 数字 . - _ ；其余（含 / \ : 控制字符 中文）一律换成 _
            if(std::isalnum(u) || c == '.' || c == '-' || c == '_'){
               out.push_back(c);
            }else{
               out.push_back('_');
            }
         }
         // 全是点的名字（"." ".." "..."）会变成路径穿越，直接拒
         if(out.find_first_not_of('.') == std::string::npos) return "";
         return out;
      }

      std::string HTTPServer::extensionOf(const std::string& name){ //寻找扩展名
         size_t dot = name.find_last_of('.');
         if(dot == std::string::npos) return "";      // 没有点，无扩展名
         if(dot == 0) return "";                      // ".bashrc" 这种不算扩展名
         std::string ext = name.substr(dot);
         if(ext.size() > 10) return "";               // 太长的不是扩展名，是文件名里的点
         return ext;
      }

      bool HTTPServer::isUploadRequest(const std::string& request){  //判断request头里是否是上传请求
         // 第一行形如 "PUT /upload/app.apk HTTP/1.1"
         if(request.compare(0, 4, "PUT ") != 0) return false;
         size_t sp = request.find(' ');
         if(sp == std::string::npos) return false;
         return request.compare(sp + 1, 8, "/upload/") == 0;
      }

      long long HTTPServer::parseContentLength(const std::string& request){  //提取content-length字段值
         std::string v = extractHeader(request, "content-length");
         if(v.empty()) return -1;
         try{
            size_t used = 0;
            long long n = std::stoll(v, &used);
            if(used != v.size() || n < 0) return -1;    // "123abc" 不能静默变 123
            return n;
         }catch(...){
            return -1;
         }
      }

      bool HTTPServer::handleCreateRelease(int fd, const std::string& request, bool keepAlive){  //绑定上传的文件和产品和版本号
         auto fail = [&](int code, const std::string& err) -> bool{ //统一发失败响应的函数
            nlohmann::ordered_json e;  //借用库 把json对象序列化为字符串
            e["ok"]    = false;
            e["error"] = err;
            return sendResponse(fd, createResponse(e.dump(), "application/json", code, keepAlive));
         };

         const size_t headEnd = request.find("\r\n\r\n");
         if(headEnd == std::string::npos) return fail(400, "bad request");
         const std::string body = request.substr(headEnd + 4);
         if(body.empty()) return fail(400, "empty body");

         const nlohmann::json j = nlohmann::json::parse(body, nullptr, false);
         if(j.is_discarded())  return fail(400, "invalid json");
         if(!j.is_object())    return fail(400, "body must be a json object");

         auto getStr = [&](const char* key) -> std::string{  //从json对象获得相对应的value
            auto it = j.find(key);
            if(it == j.end() || !it->is_string()) return std::string();
            return it->get<std::string>();
         };
         
         // 提取
         const std::string productName = getStr("product_name");
         const std::string version     = getStr("version");
         const std::string sha         = getStr("sha256");
         const std::string note        = getStr("note");
         const std::string ch          = getStr("channel");
         const std::string channel     = ch.empty() ? "beta" : ch;

         //校验
         if(productName.empty()) return fail(400, "product_name required");
         if(version.empty())     return fail(400, "version required");
         if(sha.size() != 64)    return fail(400, "sha256 must be 64 hex chars");
         for(char c : sha){
            if(!std::isxdigit(static_cast<unsigned char>(c)))
               return fail(400, "sha256 must be 64 hex chars");
         }

         // 列宽对齐：varchar(N) 限制的是字符数不是字节数，utf8mb4 下一个汉字占 3 字节，
         // 直接比 size() 会把 22 个汉字的合法产品名误杀。所以按 UTF-8 首字节计数。
         // 不校验的后果：严格模式下 INSERT 报 1406，用户拿到 500，实际上是他的参数太长（400）。
         auto charCount = [](const std::string& s) -> std::size_t{
            std::size_t n = 0;
            for(unsigned char c : s){
               if((c & 0xC0) != 0x80) ++n;   // 多字节序列里只有首字节不是 10xxxxxx
            }
            return n;
         };
         if(charCount(productName) > 64)  return fail(400, "product_name too long (max 64 chars)");
         if(channel.size()        > 16)   return fail(400, "channel too long (max 16 chars)");
         if(charCount(note)       > 255)  return fail(400, "note too long (max 255 chars)");
         
         int va = 0;
         int vb = 0;
         int vc = 0;
         int consumed = 0;
         if(version.size() > 15
         || std::sscanf(version.c_str(), "%d.%d.%d%n", &va, &vb, &vc, &consumed) != 3
         || consumed != static_cast<int>(version.size()))   //将version拆分为三个整数
            return fail(400, "version must be x.y.z");  
         if(va < 0 || vb < 0 || vc < 0 || va > 999 || vb > 999 || vc > 999)
            return fail(400, "version segment must be 0-999");

         MySQLClient& db = mysqlClient();
         const std::string escSha = db.escape(sha);  //安全化

         ResultSet rs;
         const std::string q = "SELECT id, size, ext FROM files WHERE sha256='"
                           + escSha + "' LIMIT 1";
         if(!db.select(q, rs)) return fail(500, "db error");
         if(rs.empty())        return fail(404, "content not found");

         //读取对应的文件信息
         long long fileId   = 0;
         long long fileSize = 0;
         if(!parseInto(rs[0].at(0), fileId))   return fail(500, "bad file row");
         if(!parseInto(rs[0].at(1), fileSize)) return fail(500, "bad file row");

         const std::string ext = rs[0].at(2);

         //插入releases表，已存在就更新
      const std::string sql =
         "INSERT INTO releases "
         "(product_name, version, channel, file_id, sha256, size, ext, sort_key, note) VALUES ('"
         + db.escape(productName)   + "','"
         + db.escape(version)       + "','"
         + db.escape(channel)       + "',"
         + std::to_string(fileId)   + ",'"
         + escSha                   + "',"
         + std::to_string(fileSize) + ",'"
         + db.escape(ext)           + "','"
         + makeSortKey(va, vb, vc)  + "','"
         + db.escape(note)          + "') AS new "
         "ON DUPLICATE KEY UPDATE file_id=new.file_id, sha256=new.sha256, "
         "size=new.size, ext=new.ext, sort_key=new.sort_key, "
         "note=IF(new.note='', releases.note, new.note)";

      // 只执行一次：再 execute 一遍值没变，affectedRows() 会变 0，n 就失去意义
      if(!db.execute(sql)){
         std::cerr << "登记失败: " << db.errorText() << std::endl;
         return fail(500, "insert failed");
      }
      const auto n = db.affectedRows();               // 1=新建 2=覆盖 0=无变化

      nlohmann::ordered_json out;
      out["ok"]      = true;
      out["result"]  = (n == 1) ? "created" : (n == 2) ? "updated" : "unchanged";
      out["product"] = productName;
      out["version"] = version;
      out["channel"] = channel;

      return sendResponse(fd,
         createResponse(out.dump(), "application/json", 200, keepAlive));
      }

      bool HTTPServer::handleListReleases(int fd, const std::string& query, bool keepAlive){  //验参 → 拼 SQL → 查库 → 转 JSON → 发响应
         auto fail = [&](int code, const std::string& err) -> bool{
            nlohmann::ordered_json e;
            e["ok"]    = false;
            e["error"] = err;
            return sendResponse(fd, createResponse(e.dump(), "application/json", code, keepAlive));
         };

         // ── 1. product_name 必填，query 里的中文是 percent-encoded 的（'+' 当空格）
         std::string rawProduct;
         if(!queryGet(query, "product_name", rawProduct))
            return fail(400, "product_name required");

         std::string productName;
         if(!urlDecodeQuery(rawProduct, productName) || productName.empty())
            return fail(400, "product_name invalid");

         // ── 2. sort=version 走版本序，其余走时间序。order 两个取值都是字面量，不可注入
         std::string sort;
         queryGet(query, "sort", sort);
         const std::string order = (sort == "version") ? "sort_key DESC, id DESC"
                                                      : "updated_at DESC, id DESC";

         // ── 3. 分页：limit 默认 20 上限 100。parseInto 失败时会留下半截结果，
         //    所以先接临时变量，成功才覆盖默认值
         long long limit  = 20;
         long long offset = 0;
         long long v = 0;
         std::string s;
         if(queryGet(query, "limit",  s) && parseInto(s, v)) limit  = v;
         v = 0;
         if(queryGet(query, "offset", s) && parseInto(s, v)) offset = v;
         if(limit <= 0)  limit  = 20;
         if(limit > 100) limit  = 100;
         if(offset < 0)  offset = 0;

         const std::string sql =
            "SELECT version, channel, sha256, size, ext, note, created_at, updated_at "
            "FROM releases WHERE product_name='" + mysqlClient().escape(productName) + "' "
            "ORDER BY " + order +
            " LIMIT "  + std::to_string(limit) +
            " OFFSET " + std::to_string(offset);

         ResultSet rs;
         if(!mysqlClient().select(sql, rs)) return fail(500, "db error");

         const int vi  = rs.indexOf("version");
         const int chi = rs.indexOf("channel");
         const int shi = rs.indexOf("sha256");
         const int si  = rs.indexOf("size");
         const int ei  = rs.indexOf("ext");
         const int ni  = rs.indexOf("note");
         const int cai = rs.indexOf("created_at");
         const int upi = rs.indexOf("updated_at");
         if(vi < 0 || chi < 0 || shi < 0 || si < 0 ||
            ei < 0 || ni < 0 || cai < 0 || upi < 0){
            return fail(500, "bad columns");
         }

         // ── 4. size 用 parseInto 不用 stoll：解析失败退化成 0，不抛异常
         nlohmann::ordered_json items = nlohmann::ordered_json::array();
         for(size_t i = 0; i < rs.size(); ++i){
            const Row& r = rs[i];
            long long oneSize = 0;
            parseInto(r.at(si), oneSize);

            nlohmann::ordered_json it;
            it["version"]    = r.at(vi);
            it["channel"]    = r.at(chi);
            it["size"]       = oneSize;
            it["note"]       = r.at(ni);
            it["created_at"] = r.at(cai);
            it["updated_at"] = r.at(upi);
            it["url"]        = "/download/apps/" + r.at(shi) + r.at(ei);
            // 与前端 pkgURL() 同一套规则：只有非 beta 才带 channel，
            // 否则 pkg 链接会落到后端默认的 beta 上，prod 的版本必然 404
            it["pkg"]        = "/pkg/" + productName + "/" + r.at(vi)
                             + (r.at(chi) == "beta" ? "" : "?channel=" + r.at(chi));
            items.push_back(std::move(it));
         }

         nlohmann::ordered_json out;
         out["ok"]      = true;
         out["product"] = productName;
         out["limit"]   = limit;
         out["offset"]  = offset;
         out["items"]   = std::move(items);

         // dump 带 replace：productName 来自 URL，可能不是合法 UTF-8
         return sendResponse(fd,
            createResponse(out.dump(-1, ' ', false, nlohmann::json::error_handler_t::replace),
                           "application/json", 200, keepAlive));
      }


      void HTTPServer::handlePkg(int fd, const std::string& userPath, const std::string& query, bool keepAlive){  //处理pkg(携带版本名称的访问)，然后查表得到下载地址 返回302临时重定向
         // /pkg/<product_name>/<version>?channel=prod
         // userPath 在 processRequest 里已经被 '?' 截断过一次，查询串只能从 query 拿。
         // 早期版本在这里 rest.find('?') 恒为 npos，channel 永远退化成 beta —— 那段分支是死代码。
         std::string rest = userPath.substr(5);

         std::string channelRaw;
         std::string channel;
         // 查询串里 '+' 是空格，路径段里 '+' 是字面量 —— 两个 decode 函数不能互换
         if(queryGet(query, "channel", channelRaw) && !channelRaw.empty()){
            if(!urlDecodeQuery(channelRaw, channel)) channel.clear();
         }
         // 缺省 beta 必须写在 C++ 里：SQL 里没有 "DEFAULT" 这回事 —— 那是 INSERT 的语义，这里是 WHERE。
         if(channel.empty()) channel = "beta";
         // channel 列是 VARCHAR(16)，超长在严格模式下 execute 失败 → 500。客户端参数太长是 400，不是服务器错。
         if(channel.size() > 16){ sendDownloadError(fd, 400, "400 Bad Request", keepAlive); return; }

         size_t slash = rest.find('/');
         if(slash == std::string::npos){ sendDownloadError(fd, 404, "404 Not Found", keepAlive); return; }
         // 中文产品名在 URL 里是 percent-encoded 的，必须先解回来（实现见本节末尾）
         std::string productName;
         if(!urlDecodePath(rest.substr(0, slash), productName)){
            sendDownloadError(fd, 400, "400 Bad Request", keepAlive);
            return;
         }
         std::string version = rest.substr(slash + 1);
         if(productName.empty() || version.empty()){ sendDownloadError(fd, 404, "404 Not Found", keepAlive); return; }

         // ── 参数合法了才去占坑。限流必须在 sub#0 调（Redis 异步客户端只绑在那个 epoll 上），
         //    所以这里和 download 路径同构：worker → sub#0 → 回线程池。
         uint64_t gen = 0;
         {
            std::lock_guard<std::mutex> lk(connMutex_);
            auto it = connGen_.find(fd);
            if(it == connGen_.end()) return;              // 连接已经没了
            gen = it->second;
            pendingConn_.insert(fd);                      // 告诉 sweep：这个 fd 在等 Redis，别杀
         }
         const std::string ip = peerIp(fd);

         subReactors_[0]->post([this, fd, gen, ip, productName, version, channel, keepAlive]{
            // 独立桶：配额 100/50，和下载的 20/10 互不占用
            checkRatelimit("ratelimit:pkg:" + ip, 1, kPkgRatelimitBurst, kPkgRatelimitQps,
               [this, fd, gen, productName, version, channel, keepAlive](bool ok, long long){
                  threadPool.enqueue([this, fd, gen, productName, version, channel, keepAlive, ok]{
                     PendingGuard guard{this, fd, gen};

                     {
                        std::lock_guard<std::mutex> lk(connMutex_);
                        auto it = connGen_.find(fd);
                        if(it == connGen_.end() || it->second != gen) return;   // fd 已经复用成别的连接
                     }

                     if(!ok){
                        // sendDownloadError 内部已经 finishOnce，这里不能再调
                        sendDownloadError(fd, 429, "429 Too Many Requests", keepAlive);
                        return;
                     }

                     // 这里必须查库，而且必须转义 —— product_name/version 来自 URL，用户可控
                     std::string sql = "SELECT sha256, ext FROM releases WHERE product_name='"
                                    + mysqlClient().escape(productName) + "' AND version='"
                                    + mysqlClient().escape(version) + "' AND channel='"
                                    + mysqlClient().escape(channel) + "' ORDER BY id DESC LIMIT 1";
                     ResultSet rs;
                     if(!mysqlClient().select(sql, rs) || rs.empty()){
                        sendDownloadError(fd, 404, "404 Not Found", keepAlive);
                        return;
                     }
                     const int si = rs.indexOf("sha256");
                     const int ei = rs.indexOf("ext");
                     if(si < 0 || ei < 0){ sendDownloadError(fd, 500, "500 Bad Column", keepAlive); return; }

                     std::string location = "/download/apps/" + rs[0].at(si) + rs[0].at(ei);

                     // 302 没有 body，不能走 createResponse（会塞 Content-Length 和正文）
                     std::ostringstream resp;
                     resp << "HTTP/1.1 302 Found\r\n"
                        << "Date: " << httpDate(std::time(nullptr)) << "\r\n"
                        << "Location: " << location << "\r\n"
                        << "Content-Length: 0\r\n"
                        << "Connection: " << (keepAlive ? "keep-alive" : "close") << "\r\n\r\n";

                     if(!sendResponse(fd, resp.str())){
                        closeConnection(fd);
                        return;
                     }
                     finishOnce(fd);
                  });
               });
         });
      }



      bool HTTPServer::beginRecv(int fd, const std::string& request, std::string& buf){  //用于处理 已经处理完请求头后缓冲区里剩下的请求体并登记recvFileStates_ 
                                                                                          //  在处理完请求头后调用 
         // ── 1. 校验三件事，任何一条不过就当场回错误，别建文件
         if(extractHeader(request, "transfer-encoding").size() > 0){
            sendDownloadError(fd, 400, "400 Chunked Not Supported", false);
            return false;
         }
         long long len = parseContentLength(request);
         if(len < 0){
            sendDownloadError(fd, 411, "411 Length Required", false);
            return false;
         }
         if(len > kMaxUploadSize){
            sendDownloadError(fd, 413, "413 Payload Too Large", false);
            return false;
         }

         // ── 2. 文件名：先截 query，再取 basename，最后白名单过滤
         //  /upload/app.apk?v=1
         //  └──┬───┘└──┬───┘└─┬─┘
         // prefix  basename query
         //    定位资源        修饰请求
         std::string rawPath = extractPath(request);
         size_t q = rawPath.find('?');                    // extractPath 不截 query，这里必须自己截
         if(q != std::string::npos) rawPath.resize(q);

         static const std::string kUploadPrefix = "/upload/";  //判断是不是upload
         if(rawPath.compare(0, kUploadPrefix.size(), kUploadPrefix) != 0){
            sendDownloadError(fd, 400, "400 Bad Path", false);
            return false;
         }
         std::string name = rawPath.substr(kUploadPrefix.size());  //截取basename

         size_t slash = name.find_last_of('/');  //只要最后一个/后面的内容
         if(slash != std::string::npos) name = name.substr(slash + 1);
         name = sanitizeFileName(name);  //清洗后的文件名称
         if(name.empty()){
            sendDownloadError(fd, 400, "400 Bad Filename", false);
            return false;
         }

         // ── 3. 建临时文件（data/tmp 下，web root 之外）
         std::string tmpPath = dataRoot + "/tmp/" + std::to_string(fd) + "_" +
                              std::to_string(std::time(nullptr)) + ".part";
         int rawFd = ::open(tmpPath.c_str(),
                           O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
         if(rawFd < 0){
            std::cerr << "上传临时文件创建失败: " << tmpPath << " : " << std::strerror(errno) << std::endl;
            sendDownloadError(fd, 500, "500 Cannot Create Temp File", false);
            return false;
         }

         // ── 3.5 回 100 Continue：curl 传大文件会先问「能收吗」，不答就干等 1 秒
         if(extractHeader(request, "expect").find("100-continue") != std::string::npos){
            static const char kContinue[] = "HTTP/1.1 100 Continue\r\n\r\n";
            ::send(fd, kContinue, sizeof(kContinue) - 1, 0);
         }

         auto st      = std::make_shared<RecvFileState>();
         st->file     = std::make_shared<TempFileOwner>(rawFd, tmpPath);
         st->fileName = name;
         st->mime = getMimeType(name);
         st->total     = len;
         st->remaining = len;
         SHA256_Init(&st->sha);

         // ─buf 里已经躺着 body 的开头，请求头已经处理完，必须先处理剩余的请求体
         if(!buf.empty() && st->remaining > 0){
            size_t take = std::min<size_t>(buf.size(), static_cast<size_t>(st->remaining));
            if(!writeAllToDisk(rawFd, buf.data(), take)){
               sendDownloadError(fd, 500, "500 Write Failed", false);
               return false;                     // TempFileOwner 析构 → close + unlink
            }
            SHA256_Update(&st->sha, buf.data(), take);
            st->remaining -= static_cast<long long>(take);
            buf.erase(0, take);
            // 多出来的字节（buf 比 body 还长）= pipelining 的下一个请求，留着
         }

         // ── 5. 小文件可能已经收完
         if(st->remaining == 0){
            return finishRecv(fd, st);          // 不再登记表，直接收尾
         }

         // ── 6. 登记，等后续 EPOLLIN
         {
            std::lock_guard<std::mutex> lk(connMutex_);
            auto it = connGen_.find(fd);
            if(it == connGen_.end()){
               return false;                      // 连接已经没了
            }
            st->gen = it->second;
            recvFileStates_[fd] = st;
            pendingConn_.insert(fd);              // 告诉 sweep：在传大文件，别按 30 秒杀
         }
         finishOnce(fd);                          // arm EPOLLIN
         return true;
      }


      void HTTPServer::continueRecv(int fd){ //用于处理已经登记在recvFileStates_里的大文件上传
         std::shared_ptr<RecvFileState> st;
         {
            std::lock_guard<std::mutex> lk(connMutex_);
            lastActive_[fd] = std::chrono::steady_clock::now();
            inFlight_.insert(fd);
            auto it = recvFileStates_.find(fd);
            if(it == recvFileStates_.end()){
               finishOnce(fd);
               return;
            }
            st = it->second;                     // 拷一份 shared_ptr，锁外期间保活
         }

         char chunk[kRecvChunk];
         while(st->remaining > 0){
            ssize_t n;
            do{
               n = ::recv(fd, chunk, sizeof(chunk), 0);
            }while(n < 0 && errno == EINTR);

            if(n < 0){
               if(errno == EAGAIN || errno == EWOULDBLOCK){
                  finishOnce(fd);                // 没数据了，释放 worker，等下次 EPOLLIN
                  return;
               }
               closeConnection(fd);              // 连接废了 → 析构删临时文件
               return;
            }
            if(n == 0){
               // 客户端关了，但还没传够 → 提前断开，不是正常完成
               std::cerr << "上传中断，剩余 " << st->remaining << " 字节" << std::endl;
               closeConnection(fd);
               return;
            }

            size_t take = std::min<size_t>(static_cast<size_t>(n),
                                          static_cast<size_t>(st->remaining));
            // recv 已经把 n 字节全部搬进 chunk 了，超出 body 的尾巴是下一个请求
            //    不存回去就是静默丢一个请求（它已经不在 socket 里了）
            if(static_cast<size_t>(n) > take){
               std::lock_guard<std::mutex> lk(connMutex_);
               connBuffers_[fd].append(chunk + take, static_cast<size_t>(n) - take);
            }
            if(!writeAllToDisk(st->file->fd, chunk, take)){
               // 先摘表：st 析构时才 unlink；这里不能 closeConnection，否则 500 发不出去
               {
                  std::lock_guard<std::mutex> lk(connMutex_);
                  recvFileStates_.erase(fd);
                  pendingConn_.erase(fd);
               }
               sendDownloadError(fd, 500, "500 Write Failed", false);
               return;
            }
            SHA256_Update(&st->sha, chunk, take);
            st->remaining -= static_cast<long long>(take);
         }

         finishRecv(fd, st);
      }


      bool HTTPServer::finishRecv(int fd, std::shared_ptr<RecvFileState> st){
         // 失败收尾：从两张表里摘掉自己。临时文件由 st 析构负责（release 过的不动）
         auto detach = [this, fd]{
            std::lock_guard<std::mutex> lk(connMutex_);
            recvFileStates_.erase(fd);
            pendingConn_.erase(fd);
         };

         // ── 1. 收尾文件：必须 fsync，否则断电后 rename 生效但内容是空的
         if(::fsync(st->file->fd) != 0){
            detach();
            sendDownloadError(fd, 500, "500 Fsync Failed", false);
            return false;
         }
         ::close(st->file->fd);
         st->file->fd = -1;                     // 防 TempFileOwner 析构重复 close

         // ── 2. 出哈希
         unsigned char digest[SHA256_DIGEST_LENGTH];  //32字节 每个字节的范围是0到255
         SHA256_Final(digest, &st->sha);
         std::string hex;
         hex.reserve(64);
         static const char* kHex = "0123456789abcdef";
         for(int i = 0; i < SHA256_DIGEST_LENGTH; ++i){
            hex.push_back(kHex[ digest[i] / 16]);
            hex.push_back(kHex[ digest[i] % 16]);
         }  //转为64字符

         // ── 3. 查重：同一个内容第二次上传，直接复用磁盘文件
         std::string ext = extensionOf(st->fileName);       // ".apk" / ".exe" / ""
         std::string finalPath = dataRoot + "/apps/" + hex + ext;    // dataRoot，不是 dirRoot
         std::string relUrl    = "/download/apps/" + hex + ext;      // 默认：按本次请求的 ext 算
         bool dedup = false; //dedup 去重

         {
            ResultSet rs;
            std::string sql = "SELECT disk_path FROM files WHERE sha256='"   +   mysqlClient().escape(hex)   +   "'";  //拼接为SQL语句
            if(mysqlClient().select(sql, rs) && !rs.empty()){ //不为空 表示sql里已经有相同的了
               dedup = true;
               ::unlink(st->file->path.c_str());       
               st->file->release();                       // 手动删除 靠析构也行
               mysqlClient().execute("UPDATE files SET ref_count = ref_count + 1 WHERE sha256='"
                                    + mysqlClient().escape(hex) + "'");


               const int dpIdx = rs.indexOf("disk_path"); //找disk_path在第几列
               if(dpIdx >= 0){
                  const std::string& storedDisk = rs[0].at(static_cast<std::size_t>(dpIdx)); //返回结果肯定只有一行 取第一行的对应列
                  size_t slash = storedDisk.find_last_of('/');
                  relUrl = "/download/apps/" +  
                           ((slash == std::string::npos) ? storedDisk : storedDisk.substr(slash + 1));  //取原来的扩展名
               }
            }
         }

         // ── 4. 首次上传：rename 进 dirRoot，然后入库
         if(!dedup){
            if(::rename(st->file->path.c_str(), finalPath.c_str()) != 0){
               std::cerr << "rename 失败: " << std::strerror(errno) << std::endl;
               detach();
               sendDownloadError(fd, 500, "500 Rename Failed", false);
               return false;
            }
            st->file->release();                          // rename 成功之后才调；失败时留 path 让析构删临时文件

            std::string sql =
               "INSERT INTO files (sha256, name, size, mime, ext, disk_path, ref_count) VALUES ('"
               + mysqlClient().escape(hex)          + "','"
               + mysqlClient().escape(st->fileName) + "',"
               + std::to_string(st->total)          + ",'"
               + mysqlClient().escape(st->mime)     + "','"
               + mysqlClient().escape(ext)          + "','"
               + mysqlClient().escape(finalPath)    + "',1) "
               "ON DUPLICATE KEY UPDATE ref_count = ref_count + 1";   // ← 为什么必须带，见下面两段
            if(!mysqlClient().execute(sql)){
               // 走到这里只可能是真故障 或者是撞了
               // 不要 unlink(finalPath)：这个路径可能是另一个并发上传刚建好的成果， 但是撞了
               std::cerr << "入库失败: " << mysqlClient().errorText() << std::endl;
               detach();                                  // 文件已 rename 成功，不能删；只摘表
               sendDownloadError(fd, 500, "500 DB Failed", false);
               return false;
            }
         }

         // ── 5. 从表里摘掉（如果是登记过的那种）
         detach();

         // ── 6. 回响应
         // 整棵交给库。dump 带 replace：fileName 来自 URL，可能不是合法 UTF-8，
         // 默认 strict 会抛 type_error.316，冒到 handleOnce 的 catch(...) 就是 500 + 断连。
         nlohmann::ordered_json out;
         out["ok"]     = true;
         out["sha256"] = hex;
         out["size"]   = st->total;
         out["dedup"]  = dedup;
         out["name"]   = st->fileName;
         out["url"]    = relUrl;

         std::string resp = createResponse(
            out.dump(-1, ' ', false, nlohmann::json::error_handler_t::replace),
            "application/json", 200, true);
         if(!sendResponse(fd, resp)){
            closeConnection(fd);
            return false;
         }
         finishOnce(fd);
         return true;
      }

      HTTPServer::RangeParse HTTPServer::parseRangeSpec(const std::string& value,
                          long long fileSize, ByteRange& out){  //解析Range 类似于Range: bytes=0-99 返回正确的end和start以及partial(是否是部分下载)
         // 不认识的写法（"items=0-10"、长度不够）一律当没带，回整份是 RFC 允许的
         if(value.size() < 7 || value.compare(0, 6, "bytes=") != 0) return HTTPServer::RangeParse::None;

         std::string spec = value.substr(6);

         // 🔴 多段（bytes=0-99,200-299）：整段忽略，回 200 整份。
         //    只发第一段是错的 —— 响应里没有 multipart/byteranges，客户端会把
         //    第一段当成完整文件存下来，静默残缺。
         if(spec.find(',') != std::string::npos) return HTTPServer::RangeParse::None;

         size_t dash = spec.find('-');
         if(dash == std::string::npos) return HTTPServer::RangeParse::None;

         const std::string lhs = spec.substr(0, dash);
         const std::string rhs = spec.substr(dash + 1);

         // 把 "123" 这种纯数字解析成 long long。不是纯数字就返回 -1（调用方当没带处理）
         auto parseNum = [](const std::string& s) -> long long{
            if(s.empty()) return -1;
            char* endp = nullptr;
            long long v = std::strtoll(s.c_str(), &endp, 10);  //第二个参数endp记录最后走到的位置
            // endp 没走到串尾 = 中间有垃圾字符（"12x" / "abc"）。
            //    不查这一下，strtoll("abc") 会返回 0，被当成 "bytes=0-" → 回 206 整份。
            if(endp != s.c_str() + s.size()) return -1;
            return v;
         };

         long long start = 0;
         long long end   = fileSize - 1;

         if(lhs.empty()){
            // "bytes=-500" → 最后 500 字节（下载器探测尾部元数据时会用）
            long long suffix = parseNum(rhs);
            if(suffix <= 0) return RangeParse::None;
            if(suffix > fileSize) suffix = fileSize;
            start = fileSize - suffix;
         }else{
            start = parseNum(lhs);
            if(start < 0) return RangeParse::None;
            if(!rhs.empty()){
               // "bytes=0-999"：要多了就截到末尾，这是 RFC 要求的
               end = parseNum(rhs);
               if(end < 0) return RangeParse::None;
               if(end > fileSize - 1) end = fileSize - 1;
            }
            // "bytes=1000-" 的 rhs 为空，end 保持 fileSize-1，什么都不用做
         }

         //  越界不是"当没带"，必须 416。
         //    回 200 的话 curl -C - 会报 33 退出（实测），续传直接失效。
         if(start < 0 || start >= fileSize || end < start) return RangeParse::Unsatisfiable;

         out.start   = start;
         out.end     = end;
         out.partial = true;
         return RangeParse::Ok;
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

                  ::mkdir(dataRoot.c_str(), 0755);                  // ./data（相对路径先建）
                  char dataResolved[PATH_MAX];
                  if(realpath(dataRoot.c_str(), dataResolved) != nullptr){
                     dataRoot = dataResolved;                       // 之后是绝对路径，和 dirRoot 对称
                  }
                  ::mkdir((dataRoot + "/apps").c_str(), 0755);      // data/apps，正式文件
                  ::mkdir((dataRoot + "/tmp").c_str(),  0755);      // data/tmp，半成品
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

   bool HTTPServer::handleListProducts(int fd, const std::string& query, bool keepAlive){
      auto fail = [&](int code, const std::string& err) -> bool{
         nlohmann::ordered_json e;
         e["ok"]    = false;
         e["error"] = err;
         return sendResponse(fd, createResponse(e.dump(), "application/json", code, keepAlive));
      };

      // ── 1. 分页：limit 默认 20 上限 100，offset 默认 0
      long long limit  = 20;
      long long offset = 0;
      long long v = 0;
      std::string sv;
      if(queryGet(query, "limit",  sv) && parseInto(sv, v)) limit  = v;
      v = 0;
      if(queryGet(query, "offset", sv) && parseInto(sv, v)) offset = v;
      if(limit <= 0)  limit  = 20;
      if(limit > 100) limit  = 100;
      if(offset < 0)  offset = 0;

      // ── 2. 产品不是独立表，只是 releases 里的一个维度，GROUP BY 派生即可。
      //    uk(product_name, version, channel) 的最左前缀正好能支撑 GROUP BY。
      //    ORDER BY 后面必须补 product_name：同一秒更新的两个产品只按时间排是不稳的。
      const std::string sql =
         "SELECT product_name, COUNT(*) AS release_count, MAX(updated_at) AS updated_at "
         "FROM releases GROUP BY product_name "
         "ORDER BY updated_at DESC, product_name "
         "LIMIT "  + std::to_string(limit) +
         " OFFSET " + std::to_string(offset);

      ResultSet rs;
      if(!mysqlClient().select(sql, rs)) return fail(500, "db error");

      const int pi = rs.indexOf("product_name");
      const int ci = rs.indexOf("release_count");
      const int ui = rs.indexOf("updated_at");
      if(pi < 0 || ci < 0 || ui < 0) return fail(500, "bad columns");

      nlohmann::ordered_json items = nlohmann::ordered_json::array();
      for(size_t i = 0; i < rs.size(); ++i){
         const Row& r = rs[i];
         long long cnt = 0;
         parseInto(r.at(ci), cnt);

         nlohmann::ordered_json it;
         it["product_name"]  = r.at(pi);
         it["release_count"] = cnt;
         it["updated_at"]    = r.at(ui);
         // 产品名可能含中文/空格，这里不做 percent-encode，
         // 浏览器和 curl 都会自己处理；要严谨的前端请再 encodeURIComponent 一次
         it["url"]           = "/api/releases?product_name=" + r.at(pi);
         items.push_back(std::move(it));
      }

      nlohmann::ordered_json out;
      out["ok"]     = true;
      out["limit"]  = limit;
      out["offset"] = offset;
      out["items"]  = std::move(items);

      return sendResponse(fd,
         createResponse(out.dump(-1, ' ', false, nlohmann::json::error_handler_t::replace),
                        "application/json", 200, keepAlive));
   }
