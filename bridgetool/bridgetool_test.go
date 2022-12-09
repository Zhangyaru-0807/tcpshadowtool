package bridgetoolpackage

import (
	. "bufio"
	"bytes"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
)

func TestBridgeOpen(t *testing.T) {
	assert := assert.New(t)
	conn, err := net.Dial("tcp4", "127.0.0.1:11088")
	assert.Nil(err)
	startupmessage := &pgproto3.StartupMessage{
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
	start := startupmessage.Encode(nil)
	_, err = conn.Write(start)
	if err != nil {
		t.Error("出错了")
	}

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

func TestBridgeNOpen(t *testing.T) {
	address := "127.0.0.1:11030"
	clientConn, err := net.ResolveTCPAddr("tcp4", address)
	assert.Nil(t, err)
	listener, err := net.ListenTCP("tcp4", clientConn)
	assert.Nil(t, err)
	defer listener.Close()

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

	conntion, err := listener.Accept()
	assert.Nil(err)
	reader := NewReader(conntion)
	buff := make([]byte, 16384)
	c, err := reader.Read(buff)
	//readseeker := bytes.NewReader(buff)
	//err := AuthResponse.Unpack(readseeker)
	//assert.Nil(err)
	//assert.IsType(&SqliProtocols{}, msgg)
	assert.True(c > 0)

	response, err := (&AuthResponse{
		Length: 313,
		Context: []Context{{
			Noname1:   2,
			Noname2:   0,
			Noname222: 0,
			Noname3:   100,
			Noname4:   101,
			Noname5:   0,
			Noname6:   6,
			Noname7:   108,
			Noname8:   "000000000000",
			Noname9:   32,
			Noname10:  "GBase server00000000000000.FC4G1",
			Noname11:  35,
			Noname12:  "SOftware serial number 0000000 0000",
			Noname13:  18,
			Noname14:  "Ol_gbasedbt1210_8 mber AAA#B000000",
			Noname15:  316,
			Noname16:  0,
			Noname17:  0,
			Noname18:  0,
			Noname19:  0,
			Noname20:  0,
			Noname21:  "000000000000000000000000",
			Noname22:  102,
			Noname23:  "000000",
			Noname24:  0,
			Noname25:  0,
			Noname26:  0,
			Noname27:  0,
		}},
	}).Pack()
	assert.Nil(err)
	_, err = conntion.Write(response)
	assert.Nil(err)
	//
	//reader = NewReader(conntion)
	//reader.Read(buff)
	//readseeker = bytes.NewReader(buff)
	//msgs, err = UnpackSqliTransmission(readseeker)
	//assert.Nil(err)
	//msgg = msgs[:2]
	//assert.IsType(&SqliDBOpen{}, msgg)
	//msgg = msgs[2:3]
	//assert.IsType(&SqliEot{}, msgg)
	//
	//dbopen, err := (&SqliDone{
	//	Warning:  21,
	//	Rows:     0,
	//	RowID:    0,
	//	SerialID: 0,
	//}).Pack()
	//assert.Nil(err)
	//dbopen, err = (&SqliCost{
	//	EstimatedRows: 1,
	//	EstimatedIO:   1,
	//}).Pack()
	//dbopen, err = (&SqliEot{}).Pack()
	//assert.Nil(err)
	//_, err = conntion.Write(dbopen)

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

	buf := make([]byte, 32)
	conntion.Read(buf)
	assert.True(buf != nil)
	re := bytes.NewReader(buf)
	t.Log(re.Len())
	t.Log(buf[0:32])
	t.Log(re)
	msgs, err := UnpackSqliTransmission(re)
	t.Log(msgs[0:4])
	assert.Nil(err)
	assert.IsType(&SqliPrepare{}, msgs[0])
	assert.IsType(&SqliNDescribe{}, msgs[1])
	assert.IsType(&SqliWantDone{}, msgs[2])
	assert.IsType(&SqliEot{}, msgs[3])

	describe := &SqliDescribe{
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
	}
	done := &SqliDone{
		Warning:  0,
		Rows:     0,
		RowID:    0,
		SerialID: 0,
	}
	cost := &SqliCost{
		EstimatedRows: 1,
		EstimatedIO:   2,
	}
	eot := &SqliEot{}
	var transmission SqliTransmission
	transmission = []SqliCommand{describe, done, cost, eot}
	bufff, err := transmission.Pack()
	assert.Nil(err)
	_, err = conntion.Write(bufff)

	//reader = NewReader(conntion)
	//reader.Read(buff)
	//readseeker = bytes.NewReader(buff)
	//msgs, err = UnpackSqliTransmission(readseeker)
	//assert.Nil(err)
	//msgg = msgs[:2]
	//assert.IsType(&SqliID{}, msgg)
	//msgg = msgs[2:3]
	//assert.IsType(&SqliCIdescribe{}, msgg)
	//msgg = msgs[3:4]
	//assert.IsType(&SqliEot{}, msgg)
	//
	//idescribe, err := (&SqliIdescribe{
	//	inputfields: 2,
	//	fields: []Sqlifields{{
	//		Type:                 2,
	//		ExtendID:             0,
	//		OwnerNameLength:      0,
	//		ExtendTypeNameLength: 0,
	//		PassByReferenceFlag:  0,
	//		alignment:            0,
	//		SourceType:           0,
	//		Length:               4,
	//	}, {
	//		Type:                 2,
	//		ExtendID:             0,
	//		OwnerNameLength:      0,
	//		ExtendTypeNameLength: 0,
	//		PassByReferenceFlag:  0,
	//		alignment:            0,
	//		SourceType:           0,
	//		Length:               4,
	//	},
	//	},
	//}).Pack()
	//assert.Nil(err)
	//idescribe, err = (&SqliEot{}).Pack()
	//assert.Nil(err)
	//_, err = conntion.Write(idescribe)
	//
	//reader = NewReader(conntion)
	//reader.Read(buff)
	//readseeker = bytes.NewReader(buff)
	//msgs, err = UnpackSqliTransmission(readseeker)
	//assert.Nil(err)
	//msgg = msgs[:2]
	//assert.IsType(&SqliID{}, msgg)
	//msgg = msgs[2:3]
	//assert.IsType(&SqliBind{}, msgg)
	//msgg = msgs[3:4]
	//assert.IsType(&SqliExecute{}, msgg)
	//msgg = msgs[4:5]
	//assert.IsType(&SqliEot{}, msgg)
	//
	//execute, err := (&SqliInsertDone{}).Pack()
	//assert.Nil(err)
	//execute, err = (&SqliDone{
	//	Warning:  0,
	//	Rows:     2,
	//	RowID:    267,
	//	SerialID: 0,
	//}).Pack()
	//assert.Nil(err)
	//execute, err = (&SqliCost{
	//	EstimatedRows: 1,
	//	EstimatedIO:   2,
	//}).Pack()
	//assert.Nil(err)
	//execute, err = (&SqliEot{}).Pack()
	//assert.Nil(err)
	//_, err = conntion.Write(execute)
	//
	//reader = NewReader(conntion)
	//reader.Read(buff)
	//readseeker = bytes.NewReader(buff)
	//msgs, err = UnpackSqliTransmission(readseeker)
	//assert.Nil(err)
	//msgg = msgs[:2]
	//assert.IsType(&SqliID{}, msgg)
	//msgg = msgs[2:3]
	//assert.IsType(&SqliRelease{}, msgg)
	//msgg = msgs[3:4]
	//assert.IsType(&SqliEot{}, msgg)
	//
	//release, err := (&SqliEot{}).Pack()
	//assert.Nil(err)
	//_, err = conntion.Write(release)

	front = pgproto3.NewFrontend(conn, nil)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ParseComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.BindComplete{}, msg)
	//msg, err = front.Receive()
	//assert.Nil(err)
	//assert.IsType(&pgproto3.NoData{}, msg)
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

func TestUnpack(t *testing.T) {
	buf := make([]byte, 16384)
	buf = []byte{0, 2, 0, 0, 0, 0, 0, 18, 115, 101, 108, 101, 99, 116, 32, 42, 32, 102, 114, 111, 109, 32, 116, 101, 115, 116, 0, 22, 0, 49, 0, 12}
	t.Log(buf)
	msgs := bytes.NewReader(buf)
	t.Log(msgs)
	t.Log(msgs.Len())
	msgg, err := UnpackSqliTransmission(msgs)
	assert.Nil(t, err)
	t.Log(msgg[0])
	t.Log(msgg[1])
	t.Log(msgg[2])
	t.Log(msgg[3])
}
