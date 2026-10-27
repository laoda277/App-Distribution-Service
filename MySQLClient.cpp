#include "MySQLClient.h"
#include <mysql/errmsg.h>      
#include <cstdlib>
#include <utility>

namespace{

bool isConnError(unsigned int e){
   return e == CR_SERVER_LOST        // 2013 连接被对端断开
       || e == CR_SERVER_GONE_ERROR   // 2006 服务器已消失
       || e == CR_CONN_HOST_ERROR;    // 2003 连不上
}

//  结果集 RAII：保证任何返回路径都会 free 控制储存在客户端的结果集的释放
struct ResGuard{
   MYSQL_RES* p;
   ~ResGuard(){ if(p) mysql_free_result(p); }
};

}


bool MySQLClient::libraryInit(){  //只需要调用一次 进程级
   return mysql_library_init(0, nullptr, nullptr) == 0;
}

MySQLClient::~MySQLClient(){
   close();
}


bool MySQLClient::connect(const std::string& host, const std::string& user,
                          const std::string& passwd, const std::string& db,
                          int port){
   close();

   host_ = host; user_ = user; passwd_ = passwd; db_ = db; port_ = port;

   conn_ = mysql_init(nullptr); //链接级
   if(!conn_) return false;

   unsigned int sec = 3;
   mysql_options(conn_, MYSQL_OPT_CONNECT_TIMEOUT, &sec);   //  不设的话 MySQL 挂了要卡 30 秒
   mysql_options(conn_, MYSQL_OPT_READ_TIMEOUT,    &sec);
   mysql_options(conn_, MYSQL_OPT_WRITE_TIMEOUT,   &sec);
   mysql_options(conn_, MYSQL_SET_CHARSET_NAME, "utf8mb4"); //  建连时就协商好字符集

   if(!mysql_real_connect(conn_, host_.c_str(), user_.c_str(), passwd_.c_str(),
                          db_.c_str(), port_, nullptr, 0)){
      close(); 
      lastFail_ = std::chrono::steady_clock::now();
      return false;
   }
   return true;
}

void MySQLClient::close(){
   if(conn_){ mysql_close(conn_); conn_ = nullptr; }
}


bool MySQLClient::reconnect(){
   auto now = std::chrono::steady_clock::now();
   if(now - lastFail_ < std::chrono::seconds(1)) return false;  //带1秒冷却
   return connect(host_, user_, passwd_, db_, port_);
}

bool MySQLClient::runQuery(const std::string& sql){
   if(!conn_ && !reconnect()) return false;
   if(mysql_query(conn_, sql.c_str()) == 0) return true;  //仅仅执行语句

   if(!isConnError(mysql_errno(conn_))) return false;  // 链接有错误不许重试

   close();
   if(!reconnect()) return false;
   return mysql_query(conn_, sql.c_str()) == 0;  //错误了就断开重连 再执行一次 不行就说明真的不行
}

//  store_result 返回 NULL 有两种含义，必须区分
bool MySQLClient::select(const std::string& sql, ResultSet& rs){
   rs.clear();
   if(!runQuery(sql)) return false;

   ResGuard res{ mysql_store_result(conn_) }; //构造的时候顺便执行了 把内核缓冲区数据拉到本地
   if(!res.p){
      return mysql_field_count(conn_) == 0;   //看链接层面返回的有多少列
   }

   unsigned int n = mysql_num_fields(res.p);  //本地结果集有多少列
    
   //收集全部列名并存到结rs
    MYSQL_FIELD* fields = mysql_fetch_fields(res.p); //一次取全部列名 （加了s）
    rs.names.reserve(n);
    for(unsigned int i = 0; i < n; ++i){
        rs.names.emplace_back(std::string(fields[i].name, fields[i].name_length)); //string构造函数 指针+长度
    }

    //收集全部行并依次存入rs
   while(MYSQL_ROW r = mysql_fetch_row(res.p)){  //每次返回一行数据并将游标下移一行
      unsigned long* lens = mysql_fetch_lengths(res.p);   //游标所在行有多少数据
      Row row;
      row.cols.reserve(n);
      for(unsigned int i = 0; i < n; ++i){
         row.cols.emplace_back(r[i] ? std::string(r[i], lens[i]) : std::string());
      }
      rs.rows.push_back(std::move(row));
   }
   return true;
}

bool MySQLClient::execute(const std::string& sql){
   return runQuery(sql);
}

std::string MySQLClient::escape(const std::string& in){
   if(!conn_) return {};
   std::string out(in.size() * 2 + 1, '\0'); //最坏情况是每个字符都需要转义
   unsigned long n = mysql_real_escape_string( //转义用户输入为合法SQL语句
         conn_, out.data(), in.data(), static_cast<unsigned long>(in.size()));
   out.resize(n);
   return out;
}

unsigned long long MySQLClient::affectedRows() const{
   return conn_ ? mysql_affected_rows(conn_) : 0;
}

unsigned int MySQLClient::errorCode() const{
   return conn_ ? mysql_errno(conn_) : CR_CONNECTION_ERROR;
}

std::string MySQLClient::errorText() const{
   return conn_ ? mysql_error(conn_) : "no connection";
}

namespace{
// 本线程的 MySQL 线程资源，线程退出时释放
struct ThreadGuard{
   ~ThreadGuard(){ mysql_thread_end(); }
};
}

MySQLClient& mysqlClient(){
   thread_local ThreadGuard guard;     // 先构造 → 后析构
   thread_local MySQLClient client;    // 后构造 → 先析构（先 mysql_close）

   if(!client.valid()){  //懒初始化： 只有第一次被调用才connect
      client.connect("127.0.0.1",
                     "filestore",
                     "filestore_dev_7k3m",
                     "filestore");
   }
   return client;
}