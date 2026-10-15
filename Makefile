CXX = g++
CXXFLAGS = -Wall -O2 -pthread -std=c++17  #加旗子
TARGET = httpserver

SRCS = $(wildcard *.cpp) #wild是内置函数 将查找到的按空格分隔返回
OBJS = $(SRCS:.cpp=.o)

all:$(TARGET)

$(TARGET):$(OBJS) #依赖
	$(CXX) $(OBJS) -o $@
#$@指向目标
%.o:%.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@
#$<是第一个依赖
clean:
	rm -f $(TARGET) $(OBJS)

.PHONY: all clean #屏蔽all和clean命名的文件
