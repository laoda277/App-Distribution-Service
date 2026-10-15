#include "HTTPServer.h"
#include <iostream>
#include <string>
   int main(int argc, char* argv[]){
      int port = 8080;
      if(argc > 1){
         port = std::stoi(argv[1]);
      }

      HTTPServer server(port);

      if( !server.start()){
         std::cerr<<"服务器启动失败"<<std::endl;
         return 1;
      }

      return 0;



   }