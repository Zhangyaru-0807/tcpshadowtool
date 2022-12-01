package main

import (
	"bytes"
	"net"
	"os/exec"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
)

func TestPStartupmessage(t *testing.T) {
	assert := assert.New(t)
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
		conn:    write,
		responder: func() ([]byte, error) {
			return exec.Command("sh", "", options.responseCommand).CombinedOutput()
		},
	}
	go func() {
		err := backend.handleStartup()
		if err != nil {
			t.Error("出错了")
		}
		//t.Error("why跑不到这里")
	}()
	func() {
		_, err := conn.Write(start)
		if err != nil {
			t.Error("出错了")
		}
	}()
	buff := make([]byte, 16384)
	buf := bytes.NewBuffer(buff)
	_, err := conn.Read(buf.Bytes())
	assert.Nil(err)
	front := pgproto3.NewFrontend(buf, nil)
	msg, err := front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.AuthenticationOk{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ParameterStatus{}, msg)
	//m := backend.backend.Send
	//pgproto3.Backend{m}
	//assert.Equal(msg, msg, "yinggaixiangdeng")

	//read, err := conn.Read(s)
	//if err != nil {
	//	return
	//}

}
