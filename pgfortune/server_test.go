package main

import (
	"bytes"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"net"
	"os/exec"
	"testing"
)

/*
测试输入从write传进去,传到pipe的另一端conn，随后从write接收响应
服务端从conn接收数据并从，匹配响应的响应再发到conn。
*/
func TestPgStartupmessage(t *testing.T) {
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
		err := backend.Run()
		if err != nil {
			t.Error("出错了")
		}
	}()
	func() {
		_, err := conn.Write(start)
		if err != nil {
			t.Error("出错了")
		}
	}()
	buff := make([]byte, 16384)  //创建buffer
	buf := bytes.NewBuffer(buff) //初始化buffer
	_, err := conn.Read(buf.Bytes())
	assert.Nil(err)
	front := pgproto3.NewFrontend(buf, nil)
	msg, err := front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.AuthenticationOk{}, msg)
	for i := 1; i < 13; i++ {
		msg, err = front.Receive()
		assert.Nil(err)
		assert.IsType(&pgproto3.ParameterStatus{}, msg)
	}
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.BackendKeyData{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ReadyForQuery{}, msg)
}

func TestPgQuery(t *testing.T) {
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
	query := &pgproto3.Query{String: "select * from test"}
	q := query.Encode(nil)
	backend := &PgFortuneBackend{
		backend: pgproto3.NewBackend(write, write), //io.reader和io.writer
		conn:    write,
		responder: func() ([]byte, error) {
			return exec.Command("sh", "", options.responseCommand).CombinedOutput()
		},
	}
	go func() {
		err := backend.Run()
		if err != nil {
			t.Error("出错了")
		}
	}()
	func() {
		_, err := conn.Write(start)
		if err != nil {
			t.Error("出错了")
		}
	}()
	buff := make([]byte, 16384)  //创建buffer
	buf := bytes.NewBuffer(buff) //初始化buffer
	_, err := conn.Read(buf.Bytes())
	assert.Nil(err)
	front := pgproto3.NewFrontend(buf, nil)
	msg, err := front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.AuthenticationOk{}, msg)
	for i := 1; i < 13; i++ {
		msg, err = front.Receive()
		assert.Nil(err)
		assert.IsType(&pgproto3.ParameterStatus{}, msg)
	}
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.BackendKeyData{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ReadyForQuery{}, msg)

	func() {
		_, err := conn.Write(q)
		if err != nil {
			t.Error("出错了")
		}
	}()
	_, err = conn.Read(buf.Bytes())
	assert.Nil(err)
	front = pgproto3.NewFrontend(buf, nil)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.RowDescription{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.DataRow{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.CommandComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ReadyForQuery{}, msg)
}

func TestPgParse(t *testing.T) {
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
	parse := &pgproto3.Parse{
		Name:          "",
		Query:         "selet * from test",
		ParameterOIDs: nil,
	}
	p := parse.Encode(nil)
	backend := &PgFortuneBackend{
		backend: pgproto3.NewBackend(write, write), //io.reader和io.writer
		conn:    write,
		responder: func() ([]byte, error) {
			return exec.Command("sh", "", options.responseCommand).CombinedOutput()
		},
	}
	go func() {
		err := backend.Run()
		if err != nil {
			t.Error("出错了")
		}
	}()
	func() {
		_, err := conn.Write(start)
		if err != nil {
			t.Error("出错了")
		}
	}()
	buff := make([]byte, 16384)  //创建buffer
	buf := bytes.NewBuffer(buff) //初始化buffer
	_, err := conn.Read(buf.Bytes())
	assert.Nil(err)
	front := pgproto3.NewFrontend(buf, nil)
	msg, err := front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.AuthenticationOk{}, msg)
	for i := 1; i < 13; i++ {
		msg, err = front.Receive()
		assert.Nil(err)
		assert.IsType(&pgproto3.ParameterStatus{}, msg)
	}
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.BackendKeyData{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ReadyForQuery{}, msg)

	func() {
		_, err := conn.Write(p)
		if err != nil {
			t.Error("出错了")
		}
	}()
	_, err = conn.Read(buf.Bytes())
	assert.Nil(err)
	front = pgproto3.NewFrontend(buf, nil)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ParseComplete{}, msg)
}

func TestPgPBDES(t *testing.T) {
	assert := assert.New(t)
	conn, write := net.Pipe()
	startupmesage := (&pgproto3.StartupMessage{
		ProtocolVersion: 196608,
		Parameters: map[string]string{
			"DateStyle":          "ISO",
			"TimeZone":           "Asia/Shanghai",
			"client_encoding":    "UTF8",
			"database":           "postgres",
			"extra_float_digits": "2",
			"user":               "postgres",
		},
	}).Encode(nil)
	buffer := (&pgproto3.Parse{
		Name:          "",
		Query:         "selet * from test",
		ParameterOIDs: nil,
	}).Encode(nil)
	buffer = (&pgproto3.Bind{
		DestinationPortal:    "",
		PreparedStatement:    "",
		ParameterFormatCodes: nil,
		Parameters:           nil,
		ResultFormatCodes:    nil,
	}).Encode(buffer)
	buffer = (&pgproto3.Describe{
		ObjectType: 'P',
		Name:       "",
	}).Encode(buffer)
	buffer = (&pgproto3.Execute{
		Portal:  "",
		MaxRows: 0,
	}).Encode(buffer)
	buffer = (&pgproto3.Sync{}).Encode(buffer)

	backend := &PgFortuneBackend{
		backend: pgproto3.NewBackend(write, write), //io.reader和io.writer
		conn:    write,
		responder: func() ([]byte, error) {
			return exec.Command("sh", "", options.responseCommand).CombinedOutput()
		},
	}

	go func() {
		err := backend.Run()
		if err != nil {
			t.Error("出错了")
		}
	}()
	func() {
		_, err := conn.Write(startupmesage)
		if err != nil {
			t.Error("出错了")
		}
	}()

	front := pgproto3.NewFrontend(conn, nil)
	msg, err := front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.AuthenticationOk{}, msg)
	for i := 1; i < 13; i++ {
		msg, err = front.Receive()
		assert.Nil(err)
		assert.IsType(&pgproto3.ParameterStatus{}, msg)
	}
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.BackendKeyData{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ReadyForQuery{}, msg)

	func() {
		_, err := conn.Write(buffer)
		if err != nil {
			t.Error("出错了")
		}
	}()
	front = pgproto3.NewFrontend(conn, nil)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ParseComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.BindComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.RowDescription{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.DataRow{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.CommandComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ReadyForQuery{}, msg)
}
