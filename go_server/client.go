package main

import (
	"net"
)

func sendCommand(cmd string)(string, error) {
	conn, err := net.Dial("tcp", "localhost:9090")
	if err != nil {
		return "", err
	}
	defer conn.Close()

	conn.Write([]byte(cmd+"\n"))

	buf := make([]byte, 1024)

	n, err := conn.Read(buf)
	if err != nil {
		return "", err
	}

	return string(buf[:n]), nil
}