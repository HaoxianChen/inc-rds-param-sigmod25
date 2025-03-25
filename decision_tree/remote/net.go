package remote

import (
	"context"
	"net"
	"time"
)

// WaitAddressAvailable 等待某个端口被占用，这里用dial来判断
func WaitAddressAvailable(ctx context.Context, addr string) bool {
	for {
		select {
		case <-ctx.Done():
			// 可能被取消或超时
			return false
		default:
			// 正常继续
		}
		time.Sleep(400 * time.Millisecond)
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			continue
		} else {
			conn.Close()
			break
		}
	}

	return true
}
