#include <iostream>
#include <cstring>
#include <unistd.h>
#include <arpa/inet.h>

using namespace std;

int main() {
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

        string response = "OK\n";
        send(client, response.c_str(), response.size(), 0);

        close(client);
    }

    return 0;
}