#include <iostream>
#include <cstring>
#include <unistd.h>
#include <arpa/inet.h>
#include <unordered_map>
#include <sstream>
#include <fstream>
#include <thread>
#include <mutex>
#include <cerrno>
#include <shared_mutex>
#include <deque>
#include <condition_variable>
#include <chrono>
#include <atomic>
#include <cstdlib>
#include <iomanip>
#include "threadpool.h"

#include <list>
#include <utility>

using namespace std;

unordered_map<string,string> kv;
ofstream wal("/item/MiniKV/data/wal.log", ios::app);

mutex wal_mutex;
shared_mutex kv_mutex;
mutex log_mutex;
condition_variable wal_cv;
condition_variable wal_flushed_cv;

enum class WalMode {
    Throughput,
    Reliable,
};

struct WalRecord {
    uint64_t seq;
    string line;
};

deque<WalRecord> wal_queue;
uint64_t wal_next_seq = 0;
uint64_t wal_flushed_seq = 0;

int wal_batch_size = 64;
chrono::milliseconds wal_flush_interval_ms(20);
WalMode wal_mode = WalMode::Throughput;

atomic<uint64_t> wal_flush_rounds{0};
atomic<uint64_t> accept_ok_total{0};
atomic<uint64_t> accept_fail_total{0};
atomic<uint64_t> accept_eintr_total{0};
atomic<uint64_t> client_send_fail_total{0};

ThreadPool pool(20);

void log_line(const string& message, bool is_error = false) {
    // lock_guard<mutex> lock(log_mutex);
    // if (is_error) {
    //     cerr << message << endl;
    // } else {
    //     cout << message << endl;
    // }
}

int get_env_int(const char* name, int default_value, int min_value) {
    const char* raw = getenv(name);
    if (raw == nullptr) {
        return default_value;
    }
    try {
        int value = stoi(raw);
        if (value < min_value) {
            return default_value;
        }
        return value;
    } catch (...) {
        return default_value;
    }
}

void init_wal_config() {
    wal_batch_size = get_env_int("MINIKV_WAL_BATCH_SIZE", 512, 1);
    wal_flush_interval_ms = chrono::milliseconds(get_env_int("MINIKV_WAL_FLUSH_MS", 100, 1));

    string mode = "throughput";
    if (const char* raw = getenv("MINIKV_WAL_MODE")) {
        mode = raw;
    }
    if (mode == "reliable") {
        wal_mode = WalMode::Reliable;
    } else {
        wal_mode = WalMode::Throughput;
    }

    string mode_name = (wal_mode == WalMode::Reliable) ? "reliable" : "throughput";
    log_line("WAL config: mode=" + mode_name + ", batch=" + to_string(wal_batch_size) + ", flush_ms=" + to_string(wal_flush_interval_ms.count()));
}

void flush_wal_queue_locked() {
    if (wal_queue.empty()) {
        return;
    }
    uint64_t last_seq = wal_queue.back().seq;
    while (!wal_queue.empty()) {
        wal << wal_queue.front().line << '\n';
        wal_queue.pop_front();
    }
    wal.flush();
    wal_flushed_seq = last_seq;
    wal_flush_rounds.fetch_add(1, memory_order_relaxed);
    wal_flushed_cv.notify_all();
}

void wal_flush_worker() {
    unique_lock<mutex> lock(wal_mutex);
    while (true) {
        if (wal_queue.empty()) {
            wal_cv.wait(lock, [] {
                return !wal_queue.empty();
            });
        }

        wal_cv.wait_for(lock, wal_flush_interval_ms, [] {
            return static_cast<int>(wal_queue.size()) >= wal_batch_size;
        });

        flush_wal_queue_locked();
    }
}

void append_wal_record(const string& line) {
    uint64_t seq = 0;
    {
        lock_guard<mutex> lock(wal_mutex);
        seq = ++wal_next_seq;
        wal_queue.push_back({seq, line});
    }

    wal_cv.notify_one();

    if (wal_mode == WalMode::Reliable) {
        unique_lock<mutex> lock(wal_mutex);
        wal_flushed_cv.wait(lock, [seq] {
            return wal_flushed_seq >= seq;
        });
    }
}

void monitor_metrics_thread() {
    uint64_t prev_ok = 0;
    uint64_t prev_fail = 0;
    uint64_t prev_flush_rounds = 0;
    uint64_t prev_send_fail = 0;

    while (true) {
        this_thread::sleep_for(chrono::seconds(10));

        uint64_t now_ok = accept_ok_total.load(memory_order_relaxed);
        uint64_t now_fail = accept_fail_total.load(memory_order_relaxed);
        uint64_t now_flush_rounds = wal_flush_rounds.load(memory_order_relaxed);
        uint64_t now_send_fail = client_send_fail_total.load(memory_order_relaxed);

        uint64_t delta_ok = now_ok - prev_ok;
        uint64_t delta_fail = now_fail - prev_fail;
        uint64_t delta_flush_rounds = now_flush_rounds - prev_flush_rounds;
        uint64_t delta_send_fail = now_send_fail - prev_send_fail;

        prev_ok = now_ok;
        prev_fail = now_fail;
        prev_flush_rounds = now_flush_rounds;
        prev_send_fail = now_send_fail;

        double fail_rate = 0.0;
        if ((delta_ok + delta_fail) > 0) {
            fail_rate = 100.0 * static_cast<double>(delta_fail) / static_cast<double>(delta_ok + delta_fail);
        }

        ostringstream oss;
        oss << "Monitor(10s): accept_ok=" << delta_ok
            << ", accept_fail=" << delta_fail
            << ", accept_fail_rate=" << fixed << setprecision(2) << fail_rate << "%"
            << ", wal_flush_rounds=" << delta_flush_rounds
            << ", send_fail=" << delta_send_fail;
        log_line(oss.str());
    }
}

/*
class LRUCache {
    public:
        explicit LRUCache(size_t cap): capacity(cap) {}
        bool get(const string &key, string &value){
            auto it = index.find(key);
            if(it == index.end()) return false;
            items.splice(items.begin(), items, it->second);
            value = it->second->second;
            return true;
        }
        void put(const string &key, const string &value){
            auto it = index.find(key);
            if(it != index.end()){
                it->second->second = value;
                items.splice(items.begin(), items, it->second);
                return;
            }
            items.emplace_front(key, value);
            index[key] = items.begin();
            if(items.size() > capacity){
                auto last = prev(items.end());
                index.erase(last->first);
                items.pop_back();
            }
        }
        void erase(const string &key){
            auto it = index.find(key);
            if(it != index.end()){
                items.erase(it->second);
                index.erase(it);
            }
        }
    private:
        size_t capacity;
        list<pair<string, string>> items;
        unordered_map<string, list<pair<string, string>>::iterator> index;
};

LRUCache cache(10000);
    */

void snapshot(){
    scoped_lock lock(wal_mutex, kv_mutex);

    flush_wal_queue_locked();

    wal.close(); 

    ofstream file("/item/MiniKV/data/data.db.tmp");

    for(auto &pair : kv){
        file << pair.first << ":" << pair.second << endl;
    }
    file.flush();
    file.close();
    if(rename("/item/MiniKV/data/data.db.tmp", "/item/MiniKV/data/data.db") != 0){
        log_line("Error renaming snapshot file: " + string(strerror(errno)), true);
    }

    ofstream clear("/item/MiniKV/data/wal.log", ios::trunc);
    clear.close();

    wal.clear();
    wal.open("/item/MiniKV/data/wal.log", ios::app);
    if (!wal.is_open()) {
        log_line("Failed to reopen WAL after snapshot", true);
    }

    log_line("Snapshot complete. KV size = " + to_string(kv.size()));
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
        sleep(20*60);
        snapshot();
    }
}

void load_wal(){
    ifstream file("/item/MiniKV/data/wal.log");
    string line;
    while(getline(file, line)){
        stringstream ss(line);
        string op, key;
        if(!(ss >> op >> key)) {
            continue;
        }
        if(op == "PUT"){
            string value;
            getline(ss, value);
            if(!value.empty() && value[0] == ' ') value.erase(0, 1);
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

bool read_line(int client, string &out, string &pending, size_t max_len = 4096){
    out.clear();

    while (true) {
        size_t newline_pos = pending.find('\n');
        if (newline_pos != string::npos) {
            out = pending.substr(0, newline_pos);
            pending.erase(0, newline_pos + 1);
            if (!out.empty() && out.back() == '\r') {
                out.pop_back();
            }
            return out.size() <= max_len;
        }

        if (pending.size() > max_len) {
            return false;
        }

        char buffer[1024];
        ssize_t n = recv(client, buffer, sizeof(buffer), 0);
        if (n == 0) {
            return false;
        }
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }

        pending.append(buffer, static_cast<size_t>(n));
    }
}

void handle_client(int client){

    string pending;
    while (true) {
        string cmd;
        if(!read_line(client, cmd, pending)){
            break;
        }

        log_line("Received: " + cmd);

        Command cmd_struct;
        if(!parse_command(cmd, cmd_struct)){
            string resp = "ERROR Invalid command\n";
            if(send(client, resp.c_str(), resp.size(), 0) < 0){
                client_send_fail_total.fetch_add(1, memory_order_relaxed);
                log_line("send failed: " + string(strerror(errno)), true);
                break;
            }
            continue;
        }

        string op = cmd_struct.op;
        string key = cmd_struct.key;
        string value = cmd_struct.value;
        string response;

        if(op == "PUT"){
            string wal_record = "PUT " + key + " " + value;
            append_wal_record(wal_record);

            lock_guard<shared_mutex> lock(kv_mutex);
            kv[key] = value;
            // cache.put(key, value);

            response = "OK\n";
        }

        else if(op == "GET"){

            shared_lock<shared_mutex> lock(kv_mutex);

            auto it = kv.find(key);
            if(it != kv.end()){
                response = "VALUE " + it->second + "\n";
            }else{
                response = "NOT_FOUND\n";
            }

        }

        else if(op == "DEL"){
            string wal_record = "DEL " + key;
            append_wal_record(wal_record);

            lock_guard<shared_mutex> lock(kv_mutex);

            if(kv.erase(key)){
                response = "OK\n";
                // cache.erase(key);
            }else{
                response = "NOT_FOUND\n";
            }
        }

        else {
            response = "ERROR Unsupported command\n";
        }

        if(send(client, response.c_str(), response.size(), 0) < 0){
            client_send_fail_total.fetch_add(1, memory_order_relaxed);
            log_line("send failed: " + string(strerror(errno)), true);
            break;
        }
    }

    close(client);
}

int main() {
    init_wal_config();

    load_snapshot();
    load_wal();

    thread(wal_flush_worker).detach();
    thread(monitor_metrics_thread).detach();
    thread(snapshot_thread).detach();

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        log_line("socket creation failed: " + string(strerror(errno)), true);
        return 1;
    }

    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        log_line("setsockopt(SO_REUSEADDR) failed: " + string(strerror(errno)), true);
    }

    sockaddr_in address;
    address.sin_family = AF_INET;

    int port = 9090;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_fd, (sockaddr*)&address, sizeof(address)) < 0) {
        log_line("bind failed: " + string(strerror(errno)), true);
        close(server_fd);
        return 1;
    }

    if (listen(server_fd, 256) < 0) {
        log_line("listen failed: " + string(strerror(errno)), true);
        close(server_fd);
        return 1;
    }

    log_line("C++ KV Engine running on port " + to_string(port) + "...");

    while (true) {
        int client = accept(server_fd, nullptr, nullptr);
        if (client < 0) {
            if (errno == EINTR) {
                accept_eintr_total.fetch_add(1, memory_order_relaxed);
                continue;
            }
            uint64_t fail_count = accept_fail_total.fetch_add(1, memory_order_relaxed) + 1;
            if (fail_count <= 5 || fail_count % 100 == 0) {
                log_line("accept failed: errno=" + to_string(errno) + " (" + string(strerror(errno)) + "), total_fail=" + to_string(fail_count), true);
            }
            continue;
        }
        accept_ok_total.fetch_add(1, memory_order_relaxed);
        pool.enqueue([client]{
            handle_client(client);
        });
    }

    return 0;
}