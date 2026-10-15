#pragma once

#include <ctime>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <chrono>
#include <unordered_map>

class FileCache{
public:
    explicit FileCache(size_t maxBytes = 64*1024*1024);
    std::shared_ptr<const std::string> get(const std::string& path, struct timespec* outMtime = nullptr); //第二个参数是出参 让函数同时返回内容与时间戳 用于服务器校验
                                                                                                          // 使用指针而不是引用是因为传入的参数有可能为空
    void put(const std::string& path, std::string content);

private:
    struct CacheEntry{
        std::string content;
        struct timespec mtime;   // 入库时的文件修改时间（秒+纳秒）
        std::list<std::string>::iterator lruIt;  //标识位置的list的指针
        std::chrono::steady_clock::time_point lastCheck; //上一次磁盘校验的时刻
    };

    static bool sameMtime(const struct timespec& a, const struct timespec& b);
    void evictIfNeeded();
    void removeEntryLocked(const std::string& path, const CacheEntry* expected);

    std::unordered_map<std::string, std::shared_ptr<CacheEntry>> map_; //文件路径与文件缓存项对应表
    std::list<std::string> lru_;  //根据文件路径的文件未使用时间排序表
    size_t maxBytes_; //容量上限
    size_t usedBytes_ = 0; //已占用字节数
    static constexpr size_t kMaxFileToCache = 128*1024; //允许缓存的最大文件
    static constexpr std::chrono::milliseconds kCheckInterval{2000};//检查间隔（两秒）
    std::mutex cacheMutex_;


};