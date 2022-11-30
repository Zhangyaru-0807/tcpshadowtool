package main

import (
	"github.com/jackc/pgx/v5/pgproto3"
	"net"
	"os/exec"
	"testing"
)

func TestPStartupmessage(t *testing.T) {
	//assert := assert.New(t)
	conn, write := net.Pipe()
	startupmesage := &pgproto3.StartupMessage{
		ProtocolVersion: 196608,
		Parameters: map[string]string{
			"DateStyle":          "ISO",
			"TimeZone":           "Asia/Shanghai",
			"client_encoding":    "UTF8",
			"database":           "postgres",
			"extra_float_digits": "2",
			"user":               "postgres",
		},
	}
	start := startupmesage.Encode(nil)
	backend := &PgFortuneBackend{
		backend: pgproto3.NewBackend(write, write), //io.reader和io.writer
		conn:    conn,
		responder: func() ([]byte, error) {
			return exec.Command("sh", "", options.responseCommand).CombinedOutput()
		},
	}
	go func() {
		_, err := backend.conn.Write(start)
		if err != nil {
			t.Error("出错了")
		}
	}()
	go func() {
		err := backend.handleStartup()
		if err != nil {
			t.Error("出错了")
		}
		t.Error("why跑不到这里")
	}()
	//m := backend.backend.Send
	//pgproto3.Backend{m}
	//assert.Equal(msg, msg, "yinggaixiangdeng")

	//read, err := conn.Read(s)
	//if err != nil {
	//	return
	//}

}
