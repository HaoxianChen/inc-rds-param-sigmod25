package call

import (
	"net"
	"testing"
)

func GetFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func TestRegister(t *testing.T) {
	t.Log(GetFreePort())
	conn, err := net.Dial("tcp", "127.0.0.1:2379")
	if err != nil {
		t.Log(err)
		return
	}
	conn.Close()
	ln, err := net.Listen("tcp", "172.29.68.100:2379")
	if err != nil {
		t.Log(err)
		return
	}
	ln.Close()
}
