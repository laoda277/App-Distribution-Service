#pragma once

#include <functional>
#include <cstdint>        
#include <sys/epoll.h>

class EpollEngine{

public:

    EpollEngine();
    ~EpollEngine();

    using ClientHandler = std::function<void (int)>;
    using ConnHandler = std::function<void (int, uint32_t)>;
    using TickHandler = std::function<void()>;

    bool armConnection(int fd, uint32_t events = EPOLLIN);
    bool addConnection(int fd);
    void removeConnection(int fd);
    bool init(int port, int backlog = 128);
    bool run(const ConnHandler& onConnWorking, const TickHandler& onTick = nullptr);
    bool stop();
    
    int listeningSocket() const;


private:

    int serverSocket_;
    int epollFd_;
    bool running_;
    int wakeupFd_;

    bool setNonBlocking(int fd);
    bool createListeningSocket(int port, int backlog);
    bool setupEpoll();
    bool setupEvent();
    void cleanup();
    static void sigHandle(int sig);
    inline static EpollEngine* instance_ = nullptr;

};