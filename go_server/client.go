package main

import (
    "bufio"
    "net"
    "time"
)

func sendCommand(cmd string) (string, error) {
    d := net.Dialer{Timeout: 500 * time.Millisecond}
    conn, err := d.Dial("tcp", "localhost:9090")
    if err != nil {
        return "", err
    }
    defer conn.Close()

    _ = conn.SetDeadline(time.Now().Add(800 * time.Millisecond))

    if _, err := conn.Write([]byte(cmd + "\n")); err != nil {
        return "", err
    }

    reader := bufio.NewReader(conn)
    resp, err := reader.ReadString('\n')
    if err != nil {
        return "", err
    }

    return resp, nil
}