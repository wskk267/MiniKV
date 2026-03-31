package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

type rpcConn struct {
	conn   net.Conn
	reader *bufio.Reader
}

var (
	poolOnce    sync.Once
	connPool    chan *rpcConn
	rpcPoolSize = 64
	dialTimeout = 500 * time.Millisecond
	rpcTimeout  = 2000 * time.Millisecond
)

func envInt(name string, defaultValue int, minValue int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return defaultValue
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < minValue {
		return defaultValue
	}
	return v
}

func initConnPool() {
	rpcPoolSize = envInt("MINIKV_RPC_POOL_SIZE", 256, 1)
	connPool = make(chan *rpcConn, rpcPoolSize)
}

func newRPCConn() (*rpcConn, error) {
	d := net.Dialer{Timeout: dialTimeout}
	conn, err := d.Dial("tcp", "localhost:9090")
	if err != nil {
		return nil, err
	}
	return &rpcConn{conn: conn, reader: bufio.NewReader(conn)}, nil
}

func getRPCConn() (*rpcConn, error) {
	poolOnce.Do(initConnPool)
	select {
	case c := <-connPool:
		return c, nil
	default:
		return newRPCConn()
	}
}

func putRPCConn(c *rpcConn) {
	if c == nil {
		return
	}
	poolOnce.Do(initConnPool)
	select {
	case connPool <- c:
	default:
		_ = c.conn.Close()
	}
}

func closeRPCConn(c *rpcConn) {
	if c != nil {
		_ = c.conn.Close()
	}
}

func sendCommand(cmd string) (string, error) {
	var lastErr error

	for attempt := 0; attempt < 2; attempt++ {
		c, err := getRPCConn()
		if err != nil {
			return "", err
		}

		_ = c.conn.SetDeadline(time.Now().Add(rpcTimeout))

		if _, err := c.conn.Write([]byte(cmd + "\n")); err != nil {
			closeRPCConn(c)
			lastErr = err
			continue
		}

		resp, err := c.reader.ReadString('\n')
		if err != nil {
			closeRPCConn(c)
			lastErr = err
			continue
		}

		putRPCConn(c)
		return resp, nil
	}

	if lastErr != nil {
		return "", fmt.Errorf("rpc request failed after retry: %w", lastErr)
	}
	return "", fmt.Errorf("rpc request failed")
}
