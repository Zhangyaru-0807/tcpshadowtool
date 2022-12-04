package bridgetoolpackage

import (
	. "bufio"
	"bytes"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
)

func TestBridge(t *testing.T) {
	assert := assert.New(t)
	conn, err := net.Dial("tcp4", "127.0.0.1:11088")
	assert.Nil(err)
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
	_, err = conn.Write(start)
	if err != nil {
		t.Error("出错了")
	}

	address := "127.0.0.1:11030"
	clientConn, err := net.ResolveTCPAddr("tcp4", address)
	assert.Nil(t, err)
	listener, err := net.ListenTCP("tcp4", clientConn)
	assert.Nil(t, err)
	defer listener.Close()

	conntion, err := listener.Accept()
	assert.Nil(err)
	reader := NewReader(conntion)
	buff := make([]byte, 16384)
	reader.Read(buff)
	readseeker := bytes.NewReader(buff)
	msgs, err := UnpackSqliTransmission(readseeker)
	assert.Nil(err)
	msgg := msgs[2:3]
	assert.IsType(&SqliProtocols{}, msgg)
	msgg = msgs[3:4]
	assert.IsType(&SqliProtocols{}, msgg)

	front := pgproto3.NewFrontend(conn, nil)
	msg, err := front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.AuthenticationOk{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ParameterStatus{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.BackendKeyData{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ReadyForQuery{}, msg)
}
