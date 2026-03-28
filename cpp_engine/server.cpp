#include <iostream>
#include <cstring>
#include <unistd.h>
#include <arpa/inet.h>
#include <unordered_map>
#include <sstream>
#include <fstream>
#include <thread>
#include <mutex>
#include "threadpool.h"

using namespace std;

unordered_map<string,string> kv;
ofstream wal("/item/MiniKV/data/wal.log", ios::app);

mutex wal_mutex;
mutex kv_mutex;

ThreadPool pool(8);

void snapshot(){
    scoped_lock<mutex, mutex> lock(wal_mutex, kv_mutex);
    wal.close(); 

    ofstream file("/item/MiniKV/data/data.db.tmp");

    for(auto &pair : kv){
        file << pair.first << ":" << pair.second << endl;
    }
    file.flush();
    file.close();
    if(rename("/item/MiniKV/data/data.db.tmp", "/item/MiniKV/data/data.db") != 0){
        cerr << "Error renaming snapshot file: " << strerror(errno) << endl;
    }

    ofstream clear("/item/MiniKV/data/wal.log", ios::trunc);
    clear.close();

    wal.open("/item/MiniKV/data/wal.log", ios::app);
    cout << "Snapshot complete. KV size = " << kv.size() << endl;
};

void load_snapshot(){
    ifstream file("/item/MiniKV/data/data.db");
    string line;
    while(getline(file, line)){
        auto pos = line.find(':');
        if(pos != string::npos){
            string key = line.substr(0, pos);
            string value = line.substr(pos + 1);
            kv[key] = value;
        }
    }
};

void snapshot_thread(){
    while(true){
        sleep(60);
        snapshot();
    }
}

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

struct Command{
    string op, key, value;
};
bool parse_command(const string &cmd, Command &out){
    istringstream ss(cmd);
    if(!(ss >> out.op)) return false;
    if(out.op == "GET"||out.op == "DEL"){
        if(!(ss >> out.key))return false;
        return true;
    }
    else if(out.op == "PUT"){
        if(!(ss >> out.key))return false;
        string rest;
        getline(ss, rest);
        if(!rest.empty() && rest[0] == ' ') rest.erase(0, 1);
        out.value = rest;
        return true;
    }
    return false;
}

bool read_line(int client, string &out,size_t max_len = 4096){
    out.clear();
    char c;
    while(out.size() < max_len){
        ssize_t n = recv(client, &c, 1, 0);
        if(n == 0)return false;
        if(n < 0){
            if(errno == EINTR)continue;
            return false;
        }
        if(c == '\n') return true;
        if(c != '\r') out += c;
    }
    return false;
}

void handle_client(int client){

    string cmd;

    cout << "Received: " << cmd << endl;

    Command cmd_struct;
    if(!parse_command(cmd, cmd_struct)){
        string resp="ERROR Invalid command\n";
        send(client, resp.c_str(), resp.size(), 0);
        close(client);
        return;
    }

    string op = cmd_struct.op;
    string key = cmd_struct.key;
    string value = cmd_struct.value;
    string response;

    if(op == "PUT"){

        {
            lock_guard<mutex> lock(wal_mutex);
            wal << "PUT " << key << " " << value << endl;
            wal.flush();
        }

        {
            lock_guard<mutex> lock(kv_mutex);
            kv[key] = value;
        }

        response = "OK\n";
    }

    else if(op == "GET"){

        lock_guard<mutex> lock(kv_mutex);

        if(kv.find(key) != kv.end()){
            response = "VALUE " + kv[key] + "\n";
        }else{
            response = "NOT_FOUND\n";
        }
    }

    else if(op == "DEL"){

        {
            lock_guard<mutex> lock(wal_mutex);
            wal << "DEL " << key << endl;
            wal.flush();
        }

        {
            lock_guard<mutex> lock(kv_mutex);

            if(kv.erase(key)){
                response = "OK\n";
            }else{
                response = "NOT_FOUND\n";
            }
        }
    }

    send(client, response.c_str(), response.size(), 0);

    close(client);
}

int main() {
    load_snapshot();
    load_wal();
    thread(snapshot_thread).detach();

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
        pool.enqueue([client]{
            handle_client(client);
        });
    }

    return 0;
}