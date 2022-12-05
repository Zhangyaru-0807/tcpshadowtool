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
	assert.IsType(&SqliEot{}, msgg)

	backend, err := net.Dial("tcp4", address)
	assert.Nil(err)
	buf, err := (&SqliProtocols{Protocol: nil}).Pack()
	assert.Nil(err)
	buf, err = (&SqliEot{}).Pack()
	_, err = backend.Write(buf)

	conntion, err = listener.Accept()
	assert.Nil(err)
	reader = NewReader(conntion)
	reader.Read(buff)
	readseeker = bytes.NewReader(buff)
	msgs, err = UnpackSqliTransmission(readseeker)
	assert.Nil(err)
	msgg = msgs[:2]
	assert.IsType(&SqliDBOpen{}, msgg)
	msgg = msgs[2:3]
	assert.IsType(&SqliEot{}, msgg)

	dbopen, err := (&SqliDone{
		Warning:  21,
		Rows:     0,
		RowID:    0,
		SerialID: 0,
	}).Pack()
	assert.Nil(err)
	dbopen, err = (&SqliCost{
		EstimatedRows: 1,
		EstimatedIO:   1,
	}).Pack()
	dbopen, err = (&SqliEot{}).Pack()
	assert.Nil(err)
	_, err = backend.Write(dbopen)

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
	_, err = conn.Write(buffer)
	if err != nil {
		t.Error("出错了")
	}

	conntion, err = listener.Accept()
	assert.Nil(err)
	reader = NewReader(conntion)
	reader.Read(buff)
	readseeker = bytes.NewReader(buff)
	msgs, err = UnpackSqliTransmission(readseeker)
	assert.Nil(err)
	msgg = msgs[:2]
	assert.IsType(&SqliPrepare{}, msgg)
	msgg = msgs[2:3]
	assert.IsType(&SqliNDescribe{}, msgg)
	msgg = msgs[3:4]
	assert.IsType(&SqliWantDone{}, msgg)
	msgg = msgs[4:5]
	assert.IsType(&SqliEot{}, msgg)

	prepare, err := (&SqliDescribe{
		StatementType: 2,
		StatementID:   0,
		EstimatedCost: 0,
		TupleSize:     8,
		CountOfFields: 2,
		StringTable:   8,
		Fields: []SqliField{{
			FieldIndex:              0,
			ColumnStartPos:          0,
			ColumnType:              2,
			ColumnExtendedBuiltinId: 0,
			OwnerName:               "",
			ExtendedName:            "",
			Reference:               0,
			Alignment:               0,
			SourceType:              0,
			Length:                  4,
			Name:                    "id",
		}, {
			FieldIndex:              3,
			ColumnStartPos:          4,
			ColumnType:              2,
			ColumnExtendedBuiltinId: 0,
			OwnerName:               "",
			ExtendedName:            "",
			Reference:               0,
			Alignment:               0,
			SourceType:              0,
			Length:                  4,
			Name:                    "code",
		},
		},
	}).Pack()
	assert.Nil(err)
	prepare, err = (&SqliDone{
		Warning:  0,
		Rows:     0,
		RowID:    0,
		SerialID: 0,
	}).Pack()
	assert.Nil(err)
	prepare, err = (&SqliCost{
		EstimatedRows: 1,
		EstimatedIO:   2,
	}).Pack()
	assert.Nil(err)
	prepare, err = (&SqliEot{}).Pack()
	assert.Nil(err)
	_, err = backend.Write(prepare)

	conntion, err = listener.Accept()
	assert.Nil(err)
	reader = NewReader(conntion)
	reader.Read(buff)
	readseeker = bytes.NewReader(buff)
	msgs, err = UnpackSqliTransmission(readseeker)
	assert.Nil(err)
	msgg = msgs[:2]
	assert.IsType(&SqliID{}, msgg)
	msgg = msgs[2:3]
	assert.IsType(&SqliCIdescribe{}, msgg)
	msgg = msgs[3:4]
	assert.IsType(&SqliEot{}, msgg)

	idescribe, err := (&SqliIdescribe{
		inputfields: 2,
		fields: []Sqlifields{{
			Type:                 2,
			ExtendID:             0,
			OwnerNameLength:      0,
			ExtendTypeNameLength: 0,
			PassByReferenceFlag:  0,
			alignment:            0,
			SourceType:           0,
			Length:               4,
		}, {
			Type:                 2,
			ExtendID:             0,
			OwnerNameLength:      0,
			ExtendTypeNameLength: 0,
			PassByReferenceFlag:  0,
			alignment:            0,
			SourceType:           0,
			Length:               4,
		},
		},
	}).Pack()
	assert.Nil(err)
	idescribe, err = (&SqliEot{}).Pack()
	assert.Nil(err)
	_, err = backend.Write(idescribe)

	conntion, err = listener.Accept()
	assert.Nil(err)
	reader = NewReader(conntion)
	reader.Read(buff)
	readseeker = bytes.NewReader(buff)
	msgs, err = UnpackSqliTransmission(readseeker)
	assert.Nil(err)
	msgg = msgs[:2]
	assert.IsType(&SqliID{}, msgg)
	msgg = msgs[2:3]
	assert.IsType(&SqliCurName{}, msgg)
	msgg = msgs[3:4]
	assert.IsType(&SqliBind{}, msgg)
	msgg = msgs[4:5]
	assert.IsType(&SqliOpen{}, msgg)
	msgg = msgs[5:6]
	assert.IsType(&SqliEot{}, msgg)

	open, err := (&SqliEot{}).Pack()
	assert.Nil(err)
	_, err = backend.Write(open)

	conntion, err = listener.Accept()
	assert.Nil(err)
	reader = NewReader(conntion)
	reader.Read(buff)
	readseeker = bytes.NewReader(buff)
	msgs, err = UnpackSqliTransmission(readseeker)
	assert.Nil(err)
	msgg = msgs[:2]
	assert.IsType(&SqliID{}, msgg)
	msgg = msgs[2:3]
	assert.IsType(&SqliRetType{}, msgg)
	msgg = msgs[3:4]
	assert.IsType(&SqliNFetch{}, msgg)
	msgg = msgs[4:5]
	assert.IsType(&SqliEot{}, msgg)

	nfetch, err := (&SqliTuple{
		Warnings:   0,
		size:       8,
		tupleBytes: []byte{0, 0, 0, 2, 0, 0, 0, 222},
		Values:     nil,
		fields:     nil,
	}).Pack()
	assert.Nil(err)
	nfetch, err = (&SqliDone{
		Warning:  0,
		Rows:     2,
		RowID:    267,
		SerialID: 0,
	}).Pack()
	assert.Nil(err)
	nfetch, err = (&SqliCost{
		EstimatedRows: 1,
		EstimatedIO:   2,
	}).Pack()
	assert.Nil(err)
	nfetch, err = (&SqliEot{}).Pack()
	assert.Nil(err)
	_, err = backend.Write(nfetch)

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
