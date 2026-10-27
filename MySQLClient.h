// MySQLClient.h
#pragma once
#include <string>
#include <vector>
#include <chrono>
#include <mysql/mysql.h>

struct Row{   //一行的所有列
   std::vector<std::string> cols;

   const std::string& at(std::size_t i) const{  //返回第i列内容
      static const std::string empty;
      return i < cols.size() ? cols[i] : empty;
   }
   std::size_t size() const {return cols.size(); }
};

struct ResultSet;   // 前向声明

class MySQLClient{
public:
  
   static bool libraryInit();

   MySQLClient() = default;
   ~MySQLClient();
   MySQLClient(const MySQLClient&) = delete;               //禁止拷贝构造与拷贝赋值构造
   MySQLClient& operator=(const MySQLClient&) = delete;    //防止一个close句柄被两次调用

   bool connect(const std::string& host, const std::string& user,
                const std::string& passwd, const std::string& db,
                int port = 3306);
   void close();
   bool valid() const { return conn_ != nullptr; }

   bool select(const std::string& sql, ResultSet& rs);  // 有结果集
   bool execute(const std::string& sql);                    // INSERT/UPDATE/DELETE 没有结果集

   std::string escape(const std::string& in);   // 拼 SQL 前必须过它 转义用户输入

   unsigned long long affectedRows() const;
   unsigned int errorCode() const;
   std::string errorText() const;

private:
   bool reconnect();
   bool runQuery(const std::string& sql);

   MYSQL* conn_ = nullptr;
   std::string host_, user_, passwd_, db_;
   int port_ = 3306;
   std::chrono::steady_clock::time_point lastFail_{};
};

struct ResultSet{
   std::vector<std::string> names;   // 列名，一次查询只有一份
   std::vector<Row>         rows;  //所有的行数据

   void clear(){ names.clear(); rows.clear(); }

   std::size_t size()  const { return rows.size(); } //返回行数
   bool        empty() const { return rows.empty(); } //返回行数是否为0

   const Row& operator[](std::size_t i) const { return rows[i]; } //重载运算符[] 可以[i]返回第i个行数据

   // 列名 → 下标；找不到返回 -1
   int indexOf(const std::string& name) const{
      for(std::size_t i = 0; i < names.size(); ++i){
         if(names[i] == name) return static_cast<int>(i);
      }
      return -1;
   }
};


MySQLClient& mysqlClient(); //返回对象指针