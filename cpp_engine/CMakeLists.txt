cmake_minimum_required(VERSION 3.10)

project(minikv_engine)

set(CMAKE_CXX_STANDARD 17)

add_executable(engine server.cpp)