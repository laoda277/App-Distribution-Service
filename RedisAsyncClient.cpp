#include "RedisAsyncClient.h"
#include "RedisEv.h"
#include "SubReactor.h"

#include <iostream>

namespace{
    void onReply(redisAsyncContext* ac, void* r, void* priv){ //当redis回复时候 把redisReply传给回调函数
        auto* fn = static_cast<RedisAsyncClient::ReplyFn*>(priv);
        if(fn){
            (*fn)(static_cast<redisReply*>(r));
            delete fn;
        }
        (void)ac;
    }

     bool submit(redisAsyncContext* ac, RedisAsyncClient::ReplyFn fn,
               int argc, const char** argv, const size_t* lens){    //提交命令给hiredis 
      auto* priv = new RedisAsyncClient::ReplyFn(std::move(fn));

      int rc = redisAsyncCommandArgv(ac, onReply, priv, argc, argv, lens);   //hiredis异步命令提交
      if(rc != REDIS_OK){                                                   //参数1：哪条链接 2：回调函数 3：回调函数参数 456：个数，参数，长度
         // 命令没进 obuf，hiredis 永远不会回调；不补这一次，调用方的 done 就永远不来，
         // 连接会永久挂起（既不响应也不关闭）。顺序不能反：先调回调（priv 还活着），再 delete。
         (*priv)(nullptr);
         delete priv;                                                       //回调函数参数是放在hiredis内部，等redis回复再一起发回去
         return false;
      }
      return true;
   }
}

RedisAsyncClient::~RedisAsyncClient(){
   onDisconnect_ = nullptr;        // 上层即将析构，别再回调它
   if(ac_) ac_->data = nullptr;     //
   close();
}

bool RedisAsyncClient::connect(const std::string& host, int port, SubReactor* sub, DisconnectFn onDisconnect){
   // 已经连上就只更新回调，不重建连接
   if(ac_){
      onDisconnect_ = std::move(onDisconnect);
      return true;
   }

   redisAsyncContext* ac = redisAsyncConnect(host.c_str(), port);
   if(!ac){
      std::cerr << "Redis 上下文分配失败(OOM)" << std::endl;
      return false;              // 只有 calloc 失败才会走到，无对象可释放
   }

   if(ac->err){
      std::cerr << "Redis 建连失败: " << ac->errstr << std::endl;
      redisAsyncFree(ac);        // 对象已创建，必须自己回收
      return false;
   }

   // 把 fd 挂进 subReactors_[0] 的 epoll（LT，不加 ONESHOT）
   auto* ev = new RedisEv{};
   redisEvAttach(ev, ac, sub->epollFd()); //绑定状态 ev是ac的一个版块

   ac->data = this;      //让回调函数能找到redisasyncclient对象从而执行断链接回调
   onDisconnect_ = std::move(onDisconnect);
   redisAsyncSetDisconnectCallback(ac, &RedisAsyncClient::sOnDisconnect);

   ac_ = ac;
   return true;
}

void RedisAsyncClient::close(){
   if(!ac_) return;
   redisAsyncContext* ac = ac_;   //防止disconnect的时候回调指针悬空
   ac_ = nullptr;
   redisAsyncDisconnect(ac);   // 会触发 ev.cleanup，由它 delete RedisEv
}

void RedisAsyncClient::sOnDisconnect(const redisAsyncContext* ac, int status){
   auto* self = static_cast<RedisAsyncClient*>(ac->data);
   if(!self) return;                  // 析构路径已经把 data 清了

   self->ac_ = nullptr;
   if(self->onDisconnect_) self->onDisconnect_();

   (void)status;
}


int RedisAsyncClient::fd() const{
   return ac_ ? ac_->c.fd : -1;
}


void RedisAsyncClient::handleEvent(uint32_t events){
   if(!ac_) return;

   if(events & (EPOLLERR | EPOLLHUP)){
      close();
      return;
   }

   if(events & EPOLLIN){
      redisAsyncHandleRead(ac_);   // 可能在这里面回调、甚至断连
      if(!ac_) return;
   }
   if(events & EPOLLOUT){
      redisAsyncHandleWrite(ac_);
   }
}

bool RedisAsyncClient::scriptLoad(const std::string& script, ReplyFn fn){ //发送脚本函数体 只调用一次 回调函数会返回sha1值
   if(!ac_){ fn(nullptr); return false; }

   std::vector<const char*> v{ "SCRIPT", "LOAD", script.c_str() };  //存放命令开头指针 前面参数自动识别为命令 后面为参数 语义是脚本缓存
   std::vector<size_t>      l{ 6, 4, script.size() };

   return submit(ac_, std::move(fn), 3, v.data(), l.data());  //发送给redis
}

//通过sha指纹找到redis里的lua脚本函数 然后把参数交给脚本执行命令
bool RedisAsyncClient::evalBySha(const std::string& sha,  //指纹
                               const std::vector<std::string>& keys,  //要操作的key(一个key对应一个ip)
                               const std::vector<std::string>& args,  //给lua脚本函数的参数
                               ReplyFn fn){  //回调函数只到hiredis 不会到达redis 
   if(!ac_){ fn(nullptr); return false; }

   // EVALSHA顺序： <sha> <numkeys> <key...> <arg...>
   std::vector<const char*> argv;
   std::vector<size_t>      lens;

   argv.push_back("EVALSHA");     lens.push_back(7);
   argv.push_back(sha.c_str());   lens.push_back(sha.size());

   std::string nk = std::to_string(keys.size());
   argv.push_back(nk.c_str());    lens.push_back(nk.size());

   for(const auto& k : keys){ argv.push_back(k.c_str()); lens.push_back(k.size()); }
   for(const auto& a : args){ argv.push_back(a.c_str()); lens.push_back(a.size()); }

   return submit(ac_, std::move(fn), static_cast<int>(argv.size()),
                 argv.data(), lens.data());
}

