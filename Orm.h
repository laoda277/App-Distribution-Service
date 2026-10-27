#pragma once

// Orm.h — 查询结果 → C++ 结构体
// 编译需要 -std=c++17

#include <cstddef>
#include <string>
#include <tuple>

#include "MySQLClient.h"


// 一个字段：数据库列名 + C++ 成员指针
// Field 的信息就在 &Record::member 的类型里，所以不用额外传
template<typename Record, typename Field>
struct Column{
   const char* name;
   Field Record::* member;
};

template<typename Record, typename Field>
Column(const char*, Field Record::*) -> Column<Record, Field>;


// 字符串 → C++ 类型
inline bool parseInto(const std::string& s, std::string& out){
   out = s;
   return true;
}

inline bool parseInto(const std::string& s, long long& out){
   try{
      std::size_t used = 0;
      out = std::stoll(s, &used);
      return used == s.size();   // stoll 只解析前缀，"123abc" 会静默变 123
   }catch(...){
      return false;
   }
}

inline bool parseInto(const std::string& s, int& out){
   long long v = 0;
   if(!parseInto(s, v)) return false;
   out = static_cast<int>(v);
   return true;
}


// 搬一格：按列名找下标，转换后写进成员
template<typename Record, typename Field>
bool loadOne(const ResultSet& rs, const Row& row,
             const Column<Record, Field>& col, Record& rec){
   const int i = rs.indexOf(col.name);   // 用 int 才能返回 -1
   if(i < 0) return false;
   return parseInto(row.at(static_cast<std::size_t>(i)), rec.*(col.member));
}


// 搬一行：把字段表展开成 N 次 loadOne
template<typename Record, typename Tuple>
bool loadRow(const ResultSet& rs, std::size_t rowIdx,
             Record& rec, const Tuple& cols){
   if(rowIdx >= rs.size()) return false;
   const Row& row = rs[rowIdx];
   return std::apply([&](const auto&... col){
      return (loadOne(rs, row, col, rec) && ...);   // 短路，全成功才 true
   }, cols);
}


// files 表
struct FileRecord{
   long long   id       = 0;   // 自增主键
   std::string sha256;         // 内容指纹
   std::string name;           // 原始文件名
   long long   size     = 0;   // 字节数
   std::string mime;           // Content-Type
   std::string diskPath;       // 磁盘路径（列 disk_path）
   int         refCount = 0;   // 引用计数（列 ref_count）
};

// 必须是 tuple：三个 Column 类型不同，数组装不下
inline const auto kFileCols = std::make_tuple(
   Column{"id",        &FileRecord::id},
   Column{"sha256",    &FileRecord::sha256},
   Column{"name",      &FileRecord::name},
   Column{"size",      &FileRecord::size},
   Column{"mime",      &FileRecord::mime},
   Column{"disk_path", &FileRecord::diskPath},
   Column{"ref_count", &FileRecord::refCount}
);

inline bool loadFile(const ResultSet& rs, std::size_t rowIdx, FileRecord& rec){
   return loadRow(rs, rowIdx, rec, kFileCols);
}
