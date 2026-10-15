
#include "MimeTypes.h"
#include <fstream>
#include <sstream>
#include <algorithm>
#include <cctype>

const std::vector<std::pair<std::string, std::string>> MimeTypes::defaultTypes = {
    {"html", "text/html"},
    {"htm",  "text/html"},
    {"css",  "text/css"},
    {"js",   "application/javascript"},
    {"json", "application/json"},
    {"png",  "image/png"},
    {"jpg",  "image/jpeg"},
    {"jpeg", "image/jpeg"},
    {"gif",  "image/gif"},
    {"svg",  "image/svg+xml"},
    {"ico",  "image/x-icon"},
    {"txt",  "text/plain"},
    {"xml",  "application/xml"},
    {"pdf",  "application/pdf"},
    {"zip",  "application/zip"},
    {"gz",   "application/gzip"},
    {"tar",  "application/x-tar"},
    {"mp3",  "audio/mpeg"},
    {"mp4",  "video/mp4"},
    {"woff", "font/woff"},
    {"woff2","font/woff2"},
    {"ttf",  "font/ttf"},
    {"otf",  "font/otf"},
    {"eot",  "application/vnd.ms-fontobject"},
};

bool MimeTypes::loadFromFile(const std::string& filename) {
    std::ifstream file(filename);
    if (!file.is_open()) return false;

    std::string line;
    while (std::getline(file, line)) {
       
        auto commentPos = line.find('#');
        if (commentPos != std::string::npos) line = line.substr(0, commentPos);
        line.erase(0, line.find_first_not_of(" \t"));
        if (line.empty()) continue;

        std::istringstream iss(line);
        std::string mimeType;
        if (!(iss >> mimeType)) continue;

        std::string ext;
        while (iss >> ext) {
      
            std::transform(ext.begin(), ext.end(), ext.begin(), [](unsigned char c){
                return std::tolower(c);
            });
            typeMap[ext] = mimeType;
        }
    }
    return true;
}

void MimeTypes::setDefaults() {
    typeMap.clear();
    for (const auto& pair : defaultTypes) {
        typeMap[pair.first] = pair.second;
    }
}

std::string MimeTypes::getType(const std::string& ext) const {
    auto it = typeMap.find(ext);
    if (it != typeMap.end()) return it->second;
    return "application/octet-stream"; 
}