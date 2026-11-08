CXX = g++

# 让 mysql_config 自己报头文件路径
MYSQL_CFLAGS = $(shell mysql_config --cflags)
# 自己报库路径 + 要链接的库
MYSQL_LIBS   = $(shell mysql_config --libs)
# hiredis 源码目录，提供 hiredis.h 与 async.h
HIREDIS_DIR  = $(HOME)/redis-7.2.5/deps/hiredis

# -I$(HIREDIS_DIR) 让 <hiredis.h> 和 <async.h> 能被找到
CXXFLAGS = -Wall -O2 -pthread -std=c++17 -I$(HIREDIS_DIR) -Ithird_party $(MYSQL_CFLAGS)
# 静态库全路径，避免运行时找不到 .so
LDLIBS   = $(HIREDIS_DIR)/libhiredis.a $(MYSQL_LIBS)
TARGET   = httpserver

# wildcard 是内置函数，将查找到的按空格分隔返回
SRCS = $(wildcard *.cpp)
OBJS = $(SRCS:.cpp=.o)

all: $(TARGET)

# 依赖
$(TARGET): $(OBJS)
# $(LDLIBS) 必须写在 -o $@ 之后：链接器从左往右解析，库放前面会全部失效
	$(CXX) $(OBJS) -o $@ $(LDLIBS)
# $@ 指向目标
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@
# $< 是第一个依赖
clean:
	rm -f $(TARGET) $(OBJS)

# 屏蔽 all 和 clean 命名的文件
.PHONY: all clean
