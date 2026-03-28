#include <iostream>
#include <cstring>
#include <unistd.h>
#include <arpa/inet.h>
#include <unordered_map>
#include <sstream>
#include <fstream>

using namespace std;

unordered_map<string,string> kv;
ofstream wal("/item/MiniKV/data/wal.log", ios::app);

void load_wal(){
    ifstream file("/item/MiniKV/data/wal.log");
    string line;
    while(getline(file, line)){
        stringstream ss(line);
        string op, key, value;
        ss >> op >> key;
        if(op == "PUT"){
            ss >> value;
            kv[key] = value;
        }
        else if(op == "DEL"){
            kv.erase(key);
        }   
    }
}

int main() {
    load_wal();

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address;
    address.sin_family = AF_INET;

    int port = 9090;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = INADDR_ANY;

    bind(server_fd, (sockaddr*)&address, sizeof(address));

    listen(server_fd, 5);

    cout << "C++ KV Engine running on port " << port << "..." << endl;

    while (true) {
        int client = accept(server_fd, nullptr, nullptr);

        char buffer[1024] = {0};
        read(client, buffer, 1024);
        cout << "Received: " << buffer << endl;

        string cmd(buffer);
        stringstream ss(cmd);
        string op, key, value, response;
        ss >> op >> key >> value;
        
        if(op == "PUT"){
            wal << "PUT " << key << " " << value << endl;
            wal.flush();
            kv[key] = value;
            response = "OK\n";
        }
        else if(op == "GET"){
            if(kv.find(key) != kv.end()){
                response = "VALUE "+kv[key]+"\n";
            } else {
                response = "NOT_FOUND\n";
            }
        }
        else if(op == "DEL"){
            wal << "DEL " << key << endl;
            wal.flush();
            if(kv.erase(key)){
                response = "OK\n";
            } else {
                response = "NOT_FOUND\n";
            }
        }
        else {
            response = "ERROR\n";
        }

        send(client, response.c_str(), response.size(), 0);

        close(client);
    }

    return 0;
}