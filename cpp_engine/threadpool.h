#pragma once

#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>

using namespace std;

class ThreadPool {
    public:
        explicit ThreadPool(size_t n):stop(false){
            for(size_t i = 0; i < n; i++){
                workers.emplace_back([this]{
                    while(true){
                        function<void()> task;
                        {
                            unique_lock<mutex> lock(queue_mutex);
                            condition.wait(lock, [this]{ return stop || !tasks.empty(); });
                            if(stop && tasks.empty()) return;
                            task = move(tasks.front());
                            tasks.pop();
                        }
                        task();
                    }
                });

            }
        }
        ~ThreadPool(){
            {
                lock_guard<mutex> lock(queue_mutex);
                stop = true;
            }
            condition.notify_all();
            for(thread &worker : workers){
                if(worker.joinable()) worker.join();
            }
        }
        void enqueue(function<void()> task){
            {
                lock_guard<mutex> lock(queue_mutex);
                if(stop)return;
                tasks.push(move(task));
            }
            condition.notify_one();
        }
    private:
        vector<thread> workers;
        queue<function<void()>> tasks;
        mutex queue_mutex;
        condition_variable condition;
        bool stop;
};