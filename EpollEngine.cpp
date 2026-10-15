#include "EpollEngine.h"

#include <cerrno>
#include <cstring>
#include <iostream>
#include <vector>

#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <csignal>
#include <sys/eventfd.h>
#include <cstdint>      
#include <sys/epoll.h>  


EpollEngine::EpollEngine()
: serverSocket_(-1) , epollFd_(-1) , running_(false), wakeupFd_(-1){}
EpollEngine::~EpollEngine(){
    stop();
    cleanup();
}

bool EpollEngine::setNonBlocking(int fd){
    int flags = fcntl( fd , F_GETFL , 0 );
    if(flags < 0){
        return false;
    }
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

bool EpollEngine::createListeningSocket(int port, int backlog){
    serverSocket_ = socket(AF_INET, SOCK_STREAM, 0);
    if(serverSocket_ < 0){
        std::cerr<<"创建socket失败"<<std::endl;
        return false;
    }

    int opt = 1;
    setsockopt(serverSocket_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    if(!setNonBlocking(serverSocket_)){
        std::cerr<<"设置服务器socket非阻塞失败"<<std::endl;
        return false;
    }

    sockaddr_in serverAddr;
    std::memset(&serverAddr, 0, sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_addr.s_addr = INADDR_ANY;
    serverAddr.sin_port = htons(port);

    if(bind(serverSocket_, reinterpret_cast<sockaddr*>( &serverAddr), sizeof(serverAddr)) < 0){
        std::cerr<<"绑定端口失败"<<std::endl;
        return false;
    }

    if(listen(serverSocket_, backlog) < 0){
        std::cerr<<"监听端口失败"<<std::endl;
        return false;
    }
    return true;


}

bool EpollEngine::setupEpoll(){
    epollFd_ = epoll_create1(0);
    if(epollFd_ < 0){
        std::cerr<<"创建epoll失败"<<std::endl;
        return false;
    }

    epoll_event serverEvent;
    std::memset(&serverEvent, 0, sizeof(serverEvent));
    serverEvent.events = EPOLLIN;
    serverEvent.data.fd = serverSocket_;
    if(epoll_ctl(epollFd_, EPOLL_CTL_ADD, serverSocket_, &serverEvent) < 0){
        std::cerr << "epoll注册监听socket失败" << std::endl;
        return false;
    }
    return true;
}

bool EpollEngine::setupEvent(){
    wakeupFd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    epoll_event wakeupEvent;
    std::memset(&wakeupEvent, 0, sizeof(wakeupEvent));
    wakeupEvent.events = EPOLLIN;
    wakeupEvent.data.fd = wakeupFd_;
    if(epoll_ctl(epollFd_, EPOLL_CTL_ADD, wakeupFd_, &wakeupEvent) <0){
        std::cerr<<"epoll注册监听event失败" << std::endl;
        return false;
    }
    return true;

}

bool EpollEngine::armConnection(int fd, uint32_t events) {
    epoll_event ev{};
    ev.events = events | EPOLLONESHOT;
    ev.data.fd = fd;
    return epoll_ctl(epollFd_, EPOLL_CTL_MOD, fd, &ev) == 0;
}

bool EpollEngine::addConnection(int fd){
    if(!setNonBlocking(fd)) return false;
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLONESHOT;
    ev.data.fd = fd;
    return epoll_ctl(epollFd_, EPOLL_CTL_ADD, fd, &ev) == 0;
}

void EpollEngine::removeConnection(int fd){
    epoll_ctl(epollFd_, EPOLL_CTL_DEL, fd, nullptr);
}

void EpollEngine::cleanup(){
    if(serverSocket_ >= 0){
        close(serverSocket_);
        serverSocket_ = -1;
    }
    if(epollFd_ >= 0){
        close(epollFd_);
        epollFd_  = -1;
    }
    if(wakeupFd_ >= 0){
        close(wakeupFd_);
        wakeupFd_ = -1;
    } 

}
void EpollEngine::sigHandle(int sig){
    if(instance_ && instance_->wakeupFd_ >= 0) {
    uint64_t val = 1;
    write(instance_->wakeupFd_, &val, sizeof(val));
    }
}

bool EpollEngine::init(int port, int backlog){
    cleanup();

    if(!createListeningSocket(port, backlog)){
      cleanup();
      return false;
    }

    if(!setupEpoll()){
        cleanup();
        return false;
    }
    
    if(!setupEvent()){
        cleanup();
        return false;
    }

    instance_ = this;
    struct sigaction sig;
    sig.sa_handler = EpollEngine::sigHandle;
    sigemptyset(&sig.sa_mask);
    sig.sa_flags = SA_RESTART;

    sigaction(SIGINT, &sig, nullptr);

    return true;

}

bool EpollEngine::run(const ConnHandler& onConnWorking, const TickHandler& onTick ){
    if(serverSocket_ < 0 || epollFd_ < 0){
        std::cerr<<"EpollEngine未初始化"<<std::endl;
        return false;
    }

    std::vector<epoll_event> events(16);
    running_ = true;

    while(running_){
        int ready = epoll_wait(epollFd_, events.data(), static_cast <int> (events.size()), 1000);
        if(ready < 0){
            if(errno == EINTR){
                continue;
            }
            std::cerr<<"epoll_wait失败"<<std::endl;
            return false;
        }

        for(int i = 0; i<ready; i++){
            if(events[i].data.fd == serverSocket_){
            while(true){
                sockaddr_in clientAddr;
                socklen_t clientAddrLen = sizeof(clientAddr);
                int clientSocket = accept(serverSocket_, reinterpret_cast<sockaddr*>(&clientAddr), &clientAddrLen);
                if(clientSocket < 0){
                    if(errno == EAGAIN || errno == EWOULDBLOCK){
                        break;
                    }
                    if(errno == EINTR){
                        continue;
                    }
                    std::cerr<<"接受连接失败"<<std::endl;
                    break;
                }
                addConnection(clientSocket);
            }
        }
        else if(events[i].data.fd == wakeupFd_){
            uint64_t val;
            ssize_t n = read(wakeupFd_, &val, sizeof(val));
            (void)n;
            running_ = false;
            break;
        }
        else{
            onConnWorking(events[i].data.fd, events[i].events);
        }

        }
        if(onTick) onTick();
    }
    return true;

}

bool EpollEngine::stop() {
    running_ = false;
    if(wakeupFd_ >= 0){
        uint64_t val = 1;
        write(wakeupFd_, &val, sizeof(val));
    }
    return true;
    
}
int EpollEngine::listeningSocket() const {
    return serverSocket_;
}