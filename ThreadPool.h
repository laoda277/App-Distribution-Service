#pragma once

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

class ThreadPool{
public :
    explicit ThreadPool(std::size_t threadCount);

    ~ThreadPool();

    void enqueue(std::function <void()> task);

    void drain();

private :

std::vector<std::thread > workers;
std::queue<std::function <void()>> tasks;
std::mutex queueMutex;
std::condition_variable condition;
bool stop;

};


