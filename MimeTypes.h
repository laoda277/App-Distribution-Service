#pragma once

#include <string>
#include <unordered_map>
#include <vector>

class MimeTypes {
public:
  
    bool loadFromFile(const std::string& filename);

   
    std::string getType(const std::string& ext) const;


    void setDefaults();

private:
    std::unordered_map<std::string, std::string> typeMap;


    static const std::vector<std::pair<std::string, std::string>> defaultTypes;
};
