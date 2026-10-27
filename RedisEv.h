//  hiredis 异步事件 →  SubReactor epoll
#pragma once

#include <hiredis/hiredis.h>
#include <hiredis/async.h>
#include <sys/epoll.h>
#include <unistd.h>

struct RedisEv{  //记录redis连接的情况
   redisAsyncContext* ac  = nullptr;
   int  epfd  = -1;
   int  fd    = -1;
   bool added   = false;   // fd 是否已 ADD 进 epoll
   bool reading = false;
   bool writing = false;
};

// 按 reading/writing 两个标志，同步出该有的 epoll 事件，用于回调
inline void redisEvSync(RedisEv* e){
   epoll_event ev{};
   ev.events = (e->reading ? EPOLLIN : 0) | (e->writing ? EPOLLOUT : 0);
   ev.data.fd = e->fd;

   if(ev.events == 0){ //hiredis想关闭
      if(e->added){
         epoll_ctl(e->epfd, EPOLL_CTL_DEL, e->fd, nullptr);
         e->added = false;
      }
      return;
   }

   if(!e->added){ //没有就添加
      epoll_ctl(e->epfd, EPOLL_CTL_ADD, e->fd, &ev);
      e->added = true;
   }else{  //有就修改
      epoll_ctl(e->epfd, EPOLL_CTL_MOD, e->fd, &ev);
   }
}

inline void redisEvAddRead(void* p){
   auto* e = static_cast<RedisEv*>(p);
   if(e->reading) return;
   e->reading = true;
   redisEvSync(e);
}

inline void redisEvDelRead(void* p){
   auto* e = static_cast<RedisEv*>(p);
   if(!e->reading) return;
   e->reading = false;
   redisEvSync(e);
}

inline void redisEvAddWrite(void* p){
   auto* e = static_cast<RedisEv*>(p);
   if(e->writing) return;
   e->writing = true;
   redisEvSync(e);
}

inline void redisEvDelWrite(void* p){
   auto* e = static_cast<RedisEv*>(p);
   if(!e->writing) return;
   e->writing = false;
   redisEvSync(e);
}


inline void redisEvCleanup(void* p){
   auto* e = static_cast<RedisEv*>(p);
   e->reading = false;
   e->writing = false;
   redisEvSync(e);    // 全部 DEL 掉
   delete e;
}

// 把新建的 RedisEv 挂到 ac 上；epfd 传 subReactors_[0]->epollFd()
inline bool redisEvAttach(RedisEv* e, redisAsyncContext* ac, int epfd){
    //绑定hiredis句柄里的内容到redisev
   e->ac   = ac;
   e->epfd = epfd;
   e->fd   = ac->c.fd;
   e->added = false;
   e->reading = false;
   e->writing = false;

   //绑定回调函数
   ac->ev.addRead  = redisEvAddRead;
   ac->ev.delRead  = redisEvDelRead;
   ac->ev.addWrite = redisEvAddWrite;
   ac->ev.delWrite = redisEvDelWrite;
   ac->ev.cleanup  = redisEvCleanup;
   ac->ev.data     = e;
   return true;
}