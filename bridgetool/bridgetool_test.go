package bridgetoolpackage

import (
	. "bufio"
	"bytes"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
)

func TestBridge_Select(t *testing.T) {
	address := "127.0.0.1:11030"
	clientConn, err := net.ResolveTCPAddr("tcp4", address)
	assert.Nil(t, err)
	listener, err := net.ListenTCP("tcp4", clientConn)
	assert.Nil(t, err)
	defer listener.Close()

	assert := assert.New(t)
	conn, err := net.Dial("tcp4", "127.0.0.1:11088")
	assert.Nil(err)
	//front := pgproto3.NewFrontend(conn, nil)
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
	assert.Nil(err)
	assert.True(c > 0)
	buf := buff[:c]
	readseeker := bytes.NewReader(buf)
	authrequest := &AuthRequest{}
	err = authrequest.Unpack(readseeker)

	response, err := (&AuthResponse{
		Length:           287,
		Noname1:          2,
		Noname2:          15376,
		Noname3:          0,
		Noname4:          100,
		Noname5:          101,
		Noname6:          61,
		IEEEIlength:      6,
		IEEEI:            "IEEEI",
		Noname7:          108,
		Srvinfx:          "lsrvinfx",
		Versionlength:    34,
		Version:          "GBase Server Version 9.56.FC4G1TL",
		Softwarelength:   35,
		Software:         "Software Serial Number AAA#B000000",
		Clientnamelength: 12,
		Clientname:       "gbaseserver",
		Noname8:          316,
		Noname9:          0,
		Noname10:         0,
		Noname11:         0,
		Noname12:         0,
		Noname13:         0,
		Noname14:         "on",
		Noname15:         "soctcp",
		Noname16:         102,
		Noname17:         0,
		Noname18:         0,
		Noname19:         20,
		Noname20:         0,
		Noname21:         107,
		Noname22:         3785,
		Noname23:         872,
		Noname24:         13312,
		Path1length:      11,
		Path1:            "/dev/pts/0",
		Path2length:      15,
		Path2:            "/home/gbasedbt",
		Noname25:         110,
		Noname26:         4,
		Noname27:         0,
		Noname28:         0,
		Noname29:         116,
		Noname30:         43,
		Noname31:         0,
		Noname32:         1001,
		Noname33:         0,
		Noname34:         1001,
		Path3length:      21,
		Path3:            "/home/zhangyaru/gbase/bin/oninit",
		Asceot:           127,
	}).Pack()
	assert.Nil(err)
	_, err = conntion.Write(response)
	assert.Nil(err)

	c, err = reader.Read(buff)
	assert.Nil(err)
	assert.True(c > 0)
	buf = buff[:c]
	readseeker = bytes.NewReader(buf)
	msgs, err := UnpackSqliTransmission(readseeker)
	assert.IsType(&SqliProtocols{}, msgs[0])
	assert.IsType(&SqliEot{}, msgs[1])

	protocol := []byte{0, 126, 0, 9, 189, 190, 159, 254, 127, 183, 255, 239, 240, 0}
	eot, err := (&SqliEot{}).Pack()
	assert.Nil(err)
	for _, c := range eot {
		protocol = append(protocol, c)
	}
	_, err = conntion.Write(protocol)
	assert.Nil(err)

	c, err = reader.Read(buff)
	assert.Nil(err)
	assert.True(c > 0)
	buf = buff[:c]
	readseeker = bytes.NewReader(buf)
	msgs, err = UnpackSqliTransmission(readseeker)
	assert.IsType(&SqliInfo{}, msgs[0])
	assert.IsType(&SqliEot{}, msgs[1])

	_, err = conntion.Write(eot)
	assert.Nil(err)

	c, err = reader.Read(buff)
	assert.Nil(err)
	assert.True(c > 0)
	buf = buff[:c]
	readseeker = bytes.NewReader(buf)
	msgs, err = UnpackSqliTransmission(readseeker)
	assert.IsType(&SqliDBOpen{}, msgs[0])
	assert.IsType(&SqliEot{}, msgs[1])

	done := &SqliDone{
		Warning:  21,
		Rows:     0,
		RowID:    0,
		SerialID: 0,
	}
	cost := &SqliCost{
		EstimatedRows: 1,
		EstimatedIO:   1,
	}
	eott := &SqliEot{}
	var transmission SqliTransmission
	transmission = []SqliCommand{done, cost, eott}
	buf, err = transmission.Pack()
	_, err = conntion.Write(buf)
	assert.Nil(err)

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
		Query:         "SET extra_float_digits = 3",
		ParameterOIDs: nil,
	}).Encode(nil)
	buffer = (&pgproto3.Bind{
		DestinationPortal:    "",
		PreparedStatement:    "",
		ParameterFormatCodes: nil,
		Parameters:           nil,
		ResultFormatCodes:    nil,
	}).Encode(buffer)
	buffer = (&pgproto3.Execute{
		Portal:  "",
		MaxRows: 1,
	}).Encode(buffer)
	buffer = (&pgproto3.Sync{}).Encode(buffer)
	_, err = conn.Write(buffer) //set digits
	if err != nil {
		t.Error("出错了")
	}

	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ParseComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.BindComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.CommandComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ReadyForQuery{}, msg)

	buffer = (&pgproto3.Parse{
		Name:          "",
		Query:         "SET application_name = 'PostgreSQL JDBC Driver'",
		ParameterOIDs: nil,
	}).Encode(nil)
	buffer = (&pgproto3.Bind{
		DestinationPortal:    "",
		PreparedStatement:    "",
		ParameterFormatCodes: nil,
		Parameters:           nil,
		ResultFormatCodes:    nil,
	}).Encode(buffer)
	buffer = (&pgproto3.Execute{
		Portal:  "",
		MaxRows: 0,
	}).Encode(buffer)
	buffer = (&pgproto3.Sync{}).Encode(buffer)
	_, err = conn.Write(buffer) //set JDBC
	if err != nil {
		t.Error("出错了")
	}

	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ParseComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.BindComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.CommandComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ReadyForQuery{}, msg)

	buffer = (&pgproto3.Parse{
		Name:          "",
		Query:         "select * from t",
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

	c, err = reader.Read(buff)
	buf = buff[:c]
	re := bytes.NewReader(buf)
	msgs, err = UnpackSqliTransmission(re)
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
	done = &SqliDone{
		Warning:  0,
		Rows:     0,
		RowID:    0,
		SerialID: 0,
	}
	cost = &SqliCost{
		EstimatedRows: 1,
		EstimatedIO:   2,
	}
	transmission = []SqliCommand{describe, done, cost, eott}
	buffer, err = transmission.Pack()
	assert.Nil(err)
	_, err = conntion.Write(buffer)

	c, err = reader.Read(buff)
	assert.Nil(err)
	assert.True(c > 0)
	buf = buff[:c]
	readseeker = bytes.NewReader(buf)
	msgs, err = UnpackSqliTransmission(readseeker)
	assert.IsType(&SqliID{}, msgs[0])
	assert.IsType(&SqliCIdescribe{}, msgs[1])
	assert.IsType(&SqliEot{}, msgs[2])

	idescribe := &SqliIdescribe{
		Inputfields: 2,
		Fields: []Sqlifields{{
			Type:                 2,
			ExtendID:             0,
			OwnerNameLength:      0,
			ExtendTypeNameLength: 0,
			PassByReferenceFlag:  0,
			Alignment:            0,
			SourceType:           0,
			Length:               4,
		}, {
			Type:                 2,
			ExtendID:             0,
			OwnerNameLength:      0,
			ExtendTypeNameLength: 0,
			PassByReferenceFlag:  0,
			Alignment:            0,
			SourceType:           0,
			Length:               4,
		}},
	}
	transmission = []SqliCommand{idescribe, eott}
	buffer, err = transmission.Pack()
	_, err = conntion.Write(buffer)

	c, err = reader.Read(buff)
	assert.Nil(err)
	assert.True(c > 0)
	buf = buff[:c]
	readseeker = bytes.NewReader(buf)
	msgs, err = UnpackSqliTransmission(readseeker)
	assert.IsType(&SqliID{}, msgs[0])
	assert.IsType(&SqliCurName{}, msgs[1])
	//assert.IsType(&SqliBind{}, msgs[2])
	assert.IsType(&SqliOpen{}, msgs[2])
	assert.IsType(&SqliEot{}, msgs[3])

	transmission = []SqliCommand{eott}
	buffer, err = transmission.Pack()
	assert.Nil(err)
	_, err = conntion.Write(buffer)
	assert.Nil(err)

	c, err = reader.Read(buff)
	assert.Nil(err)
	assert.True(c > 0)
	buf = buff[:c]
	readseeker = bytes.NewReader(buf)
	msgs, err = UnpackSqliTransmission(readseeker)
	assert.IsType(&SqliID{}, msgs[0])
	assert.IsType(&SqliRetType{}, msgs[1])
	assert.IsType(&SqliNFetch{}, msgs[2])
	assert.IsType(&SqliEot{}, msgs[3])

	tuple := &SqliTuple{
		Warnings: 0,
		Size:     4,
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
		},
		},
	}
	done = &SqliDone{
		Warning:  0,
		Rows:     3,
		RowID:    259,
		SerialID: 0,
	}
	cost = &SqliCost{
		EstimatedRows: 32,
		EstimatedIO:   2,
	}
	transmission = []SqliCommand{tuple, tuple, tuple, done, cost, eott}
	buffer, err = transmission.Pack()
	assert.Nil(err)
	_, err = conntion.Write(buffer)
	assert.Nil(err)

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
	assert.IsType(&pgproto3.DataRow{}, msg)
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

func TestBridge_Select_Bind(t *testing.T) {
	address := "127.0.0.1:11030"
	clientConn, err := net.ResolveTCPAddr("tcp4", address)
	assert.Nil(t, err)
	listener, err := net.ListenTCP("tcp4", clientConn)
	assert.Nil(t, err)
	defer listener.Close()

	assert := assert.New(t)
	conn, err := net.Dial("tcp4", "127.0.0.1:11088")
	assert.Nil(err)
	//front := pgproto3.NewFrontend(conn, nil)
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
	assert.Nil(err)
	assert.True(c > 0)
	buf := buff[:c]
	readseeker := bytes.NewReader(buf)
	authrequest := &AuthRequest{}
	err = authrequest.Unpack(readseeker)

	response, err := (&AuthResponse{
		Length:           287,
		Noname1:          2,
		Noname2:          15376,
		Noname3:          0,
		Noname4:          100,
		Noname5:          101,
		Noname6:          61,
		IEEEIlength:      6,
		IEEEI:            "IEEEI",
		Noname7:          108,
		Srvinfx:          "lsrvinfx",
		Versionlength:    34,
		Version:          "GBase Server Version 9.56.FC4G1TL",
		Softwarelength:   35,
		Software:         "Software Serial Number AAA#B000000",
		Clientnamelength: 12,
		Clientname:       "gbaseserver",
		Noname8:          316,
		Noname9:          0,
		Noname10:         0,
		Noname11:         0,
		Noname12:         0,
		Noname13:         0,
		Noname14:         "on",
		Noname15:         "soctcp",
		Noname16:         102,
		Noname17:         0,
		Noname18:         0,
		Noname19:         20,
		Noname20:         0,
		Noname21:         107,
		Noname22:         3785,
		Noname23:         872,
		Noname24:         13312,
		Path1length:      11,
		Path1:            "/dev/pts/0",
		Path2length:      15,
		Path2:            "/home/gbasedbt",
		Noname25:         110,
		Noname26:         4,
		Noname27:         0,
		Noname28:         0,
		Noname29:         116,
		Noname30:         43,
		Noname31:         0,
		Noname32:         1001,
		Noname33:         0,
		Noname34:         1001,
		Path3length:      21,
		Path3:            "/home/zhangyaru/gbase/bin/oninit",
		Asceot:           127,
	}).Pack()
	assert.Nil(err)
	_, err = conntion.Write(response)
	assert.Nil(err)

	c, err = reader.Read(buff)
	assert.Nil(err)
	assert.True(c > 0)
	buf = buff[:c]
	readseeker = bytes.NewReader(buf)
	msgs, err := UnpackSqliTransmission(readseeker)
	assert.IsType(&SqliProtocols{}, msgs[0])
	assert.IsType(&SqliEot{}, msgs[1])

	protocol := []byte{0, 126, 0, 9, 189, 190, 159, 254, 127, 183, 255, 239, 240, 0}
	eot, err := (&SqliEot{}).Pack()
	assert.Nil(err)
	for _, c := range eot {
		protocol = append(protocol, c)
	}
	_, err = conntion.Write(protocol)
	assert.Nil(err)

	c, err = reader.Read(buff)
	assert.Nil(err)
	assert.True(c > 0)
	buf = buff[:c]
	readseeker = bytes.NewReader(buf)
	msgs, err = UnpackSqliTransmission(readseeker)
	assert.IsType(&SqliInfo{}, msgs[0])
	assert.IsType(&SqliEot{}, msgs[1])

	_, err = conntion.Write(eot)
	assert.Nil(err)

	c, err = reader.Read(buff)
	assert.Nil(err)
	assert.True(c > 0)
	buf = buff[:c]
	readseeker = bytes.NewReader(buf)
	msgs, err = UnpackSqliTransmission(readseeker)
	assert.IsType(&SqliDBOpen{}, msgs[0])
	assert.IsType(&SqliEot{}, msgs[1])

	done := &SqliDone{
		Warning:  21,
		Rows:     0,
		RowID:    0,
		SerialID: 0,
	}
	cost := &SqliCost{
		EstimatedRows: 1,
		EstimatedIO:   1,
	}
	eott := &SqliEot{}
	var transmission SqliTransmission
	transmission = []SqliCommand{done, cost, eott}
	buf, err = transmission.Pack()
	_, err = conntion.Write(buf)
	assert.Nil(err)

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
		Query:         "SET extra_float_digits = 3",
		ParameterOIDs: nil,
	}).Encode(nil)
	buffer = (&pgproto3.Bind{
		DestinationPortal:    "",
		PreparedStatement:    "",
		ParameterFormatCodes: nil,
		Parameters:           nil,
		ResultFormatCodes:    nil,
	}).Encode(buffer)
	buffer = (&pgproto3.Execute{
		Portal:  "",
		MaxRows: 1,
	}).Encode(buffer)
	buffer = (&pgproto3.Sync{}).Encode(buffer)
	_, err = conn.Write(buffer) //set digits
	if err != nil {
		t.Error("出错了")
	}

	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ParseComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.BindComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.CommandComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ReadyForQuery{}, msg)

	buffer = (&pgproto3.Parse{
		Name:          "",
		Query:         "SET application_name = 'PostgreSQL JDBC Driver'",
		ParameterOIDs: nil,
	}).Encode(nil)
	buffer = (&pgproto3.Bind{
		DestinationPortal:    "",
		PreparedStatement:    "",
		ParameterFormatCodes: nil,
		Parameters:           nil,
		ResultFormatCodes:    nil,
	}).Encode(buffer)
	buffer = (&pgproto3.Execute{
		Portal:  "",
		MaxRows: 0,
	}).Encode(buffer)
	buffer = (&pgproto3.Sync{}).Encode(buffer)
	_, err = conn.Write(buffer) //set JDBC
	if err != nil {
		t.Error("出错了")
	}

	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ParseComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.BindComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.CommandComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ReadyForQuery{}, msg)

	buffer = (&pgproto3.Parse{
		Name:          "",
		Query:         "select * from t",
		ParameterOIDs: nil,
	}).Encode(nil)
	buffer = (&pgproto3.Bind{
		DestinationPortal:    "",
		PreparedStatement:    "",
		ParameterFormatCodes: []int16{1},
		Parameters:           [][]byte{{2}},
		ResultFormatCodes:    []int16{0},
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

	c, err = reader.Read(buff)
	buf = buff[:c]
	re := bytes.NewReader(buf)
	msgs, err = UnpackSqliTransmission(re)
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
	done = &SqliDone{
		Warning:  0,
		Rows:     0,
		RowID:    0,
		SerialID: 0,
	}
	cost = &SqliCost{
		EstimatedRows: 1,
		EstimatedIO:   2,
	}
	transmission = []SqliCommand{describe, done, cost, eott}
	buffer, err = transmission.Pack()
	assert.Nil(err)
	_, err = conntion.Write(buffer)

	c, err = reader.Read(buff)
	assert.Nil(err)
	assert.True(c > 0)
	buf = buff[:c]
	readseeker = bytes.NewReader(buf)
	msgs, err = UnpackSqliTransmission(readseeker)
	assert.IsType(&SqliID{}, msgs[0])
	assert.IsType(&SqliCIdescribe{}, msgs[1])
	assert.IsType(&SqliEot{}, msgs[2])

	idescribe := &SqliIdescribe{
		Inputfields: 2,
		Fields: []Sqlifields{{
			Type:                 2,
			ExtendID:             0,
			OwnerNameLength:      0,
			ExtendTypeNameLength: 0,
			PassByReferenceFlag:  0,
			Alignment:            0,
			SourceType:           0,
			Length:               4,
		}, {
			Type:                 2,
			ExtendID:             0,
			OwnerNameLength:      0,
			ExtendTypeNameLength: 0,
			PassByReferenceFlag:  0,
			Alignment:            0,
			SourceType:           0,
			Length:               4,
		}},
	}
	transmission = []SqliCommand{idescribe, eott}
	buffer, err = transmission.Pack()
	_, err = conntion.Write(buffer)

	c, err = reader.Read(buff)
	assert.Nil(err)
	assert.True(c > 0)
	buf = buff[:c]
	readseeker = bytes.NewReader(buf)
	msgs, err = UnpackSqliTransmission(readseeker)
	assert.IsType(&SqliID{}, msgs[0])
	assert.IsType(&SqliCurName{}, msgs[1])
	assert.IsType(&SqliBind{}, msgs[2])
	assert.IsType(&SqliOpen{}, msgs[3])
	assert.IsType(&SqliEot{}, msgs[4])

	transmission = []SqliCommand{eott}
	buffer, err = transmission.Pack()
	assert.Nil(err)
	_, err = conntion.Write(buffer)
	assert.Nil(err)

	c, err = reader.Read(buff)
	assert.Nil(err)
	assert.True(c > 0)
	buf = buff[:c]
	readseeker = bytes.NewReader(buf)
	msgs, err = UnpackSqliTransmission(readseeker)
	assert.IsType(&SqliID{}, msgs[0])
	assert.IsType(&SqliRetType{}, msgs[1])
	assert.IsType(&SqliNFetch{}, msgs[2])
	assert.IsType(&SqliEot{}, msgs[3])

	tuple := &SqliTuple{
		Warnings: 0,
		Size:     4,
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
		},
		},
	}
	done = &SqliDone{
		Warning:  0,
		Rows:     3,
		RowID:    259,
		SerialID: 0,
	}
	cost = &SqliCost{
		EstimatedRows: 32,
		EstimatedIO:   2,
	}
	transmission = []SqliCommand{tuple, tuple, tuple, done, cost, eott}
	buffer, err = transmission.Pack()
	assert.Nil(err)
	_, err = conntion.Write(buffer)
	assert.Nil(err)

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
	assert.IsType(&pgproto3.DataRow{}, msg)
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

func TestBridge_NonOpen(t *testing.T) {
	address := "127.0.0.1:11030"
	clientConn, err := net.ResolveTCPAddr("tcp4", address)
	assert.Nil(t, err)
	listener, err := net.ListenTCP("tcp4", clientConn)
	assert.Nil(t, err)
	defer listener.Close()

	assert := assert.New(t)
	conn, err := net.Dial("tcp4", "127.0.0.1:11088")
	assert.Nil(err)
	//front := pgproto3.NewFrontend(conn, nil)
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
	assert.Nil(err)
	assert.True(c > 0)
	buf := buff[:c]
	readseeker := bytes.NewReader(buf)
	authrequest := &AuthRequest{}
	err = authrequest.Unpack(readseeker)

	response, err := (&AuthResponse{
		Length:           287,
		Noname1:          2,
		Noname2:          15376,
		Noname3:          0,
		Noname4:          100,
		Noname5:          101,
		Noname6:          61,
		IEEEIlength:      6,
		IEEEI:            "IEEEI",
		Noname7:          108,
		Srvinfx:          "lsrvinfx",
		Versionlength:    34,
		Version:          "GBase Server Version 9.56.FC4G1TL",
		Softwarelength:   35,
		Software:         "Software Serial Number AAA#B000000",
		Clientnamelength: 12,
		Clientname:       "gbaseserver",
		Noname8:          316,
		Noname9:          0,
		Noname10:         0,
		Noname11:         0,
		Noname12:         0,
		Noname13:         0,
		Noname14:         "on",
		Noname15:         "soctcp",
		Noname16:         102,
		Noname17:         0,
		Noname18:         0,
		Noname19:         20,
		Noname20:         0,
		Noname21:         107,
		Noname22:         3785,
		Noname23:         872,
		Noname24:         13312,
		Path1length:      11,
		Path1:            "/dev/pts/0",
		Path2length:      15,
		Path2:            "/home/gbasedbt",
		Noname25:         110,
		Noname26:         4,
		Noname27:         0,
		Noname28:         0,
		Noname29:         116,
		Noname30:         43,
		Noname31:         0,
		Noname32:         1001,
		Noname33:         0,
		Noname34:         1001,
		Path3length:      21,
		Path3:            "/home/zhangyaru/gbase/bin/oninit",
		Asceot:           127,
	}).Pack()
	assert.Nil(err)
	_, err = conntion.Write(response)
	assert.Nil(err)

	c, err = reader.Read(buff)
	assert.Nil(err)
	assert.True(c > 0)
	buf = buff[:c]
	readseeker = bytes.NewReader(buf)
	msgs, err := UnpackSqliTransmission(readseeker)
	assert.IsType(&SqliProtocols{}, msgs[0])
	assert.IsType(&SqliEot{}, msgs[1])

	protocol := []byte{0, 126, 0, 9, 189, 190, 159, 254, 127, 183, 255, 239, 240, 0}
	eot, err := (&SqliEot{}).Pack()
	assert.Nil(err)
	for _, c := range eot {
		protocol = append(protocol, c)
	}
	_, err = conntion.Write(protocol)
	assert.Nil(err)

	c, err = reader.Read(buff)
	assert.Nil(err)
	assert.True(c > 0)
	buf = buff[:c]
	readseeker = bytes.NewReader(buf)
	msgs, err = UnpackSqliTransmission(readseeker)
	assert.IsType(&SqliInfo{}, msgs[0])
	assert.IsType(&SqliEot{}, msgs[1])

	_, err = conntion.Write(eot)
	assert.Nil(err)

	c, err = reader.Read(buff)
	assert.Nil(err)
	assert.True(c > 0)
	buf = buff[:c]
	readseeker = bytes.NewReader(buf)
	msgs, err = UnpackSqliTransmission(readseeker)
	assert.IsType(&SqliDBOpen{}, msgs[0])
	assert.IsType(&SqliEot{}, msgs[1])

	done := &SqliDone{
		Warning:  21,
		Rows:     0,
		RowID:    0,
		SerialID: 0,
	}
	cost := &SqliCost{
		EstimatedRows: 1,
		EstimatedIO:   1,
	}
	eott := &SqliEot{}
	var transmission SqliTransmission
	transmission = []SqliCommand{done, cost, eott}
	buf, err = transmission.Pack()
	_, err = conntion.Write(buf)
	assert.Nil(err)

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
		Query:         "SET extra_float_digits = 3",
		ParameterOIDs: nil,
	}).Encode(nil)
	buffer = (&pgproto3.Bind{
		DestinationPortal:    "",
		PreparedStatement:    "",
		ParameterFormatCodes: nil,
		Parameters:           nil,
		ResultFormatCodes:    nil,
	}).Encode(buffer)
	buffer = (&pgproto3.Execute{
		Portal:  "",
		MaxRows: 1,
	}).Encode(buffer)
	buffer = (&pgproto3.Sync{}).Encode(buffer)
	_, err = conn.Write(buffer) //set digits
	if err != nil {
		t.Error("出错了")
	}

	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ParseComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.BindComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.CommandComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ReadyForQuery{}, msg)

	buffer = (&pgproto3.Parse{
		Name:          "",
		Query:         "SET application_name = 'PostgreSQL JDBC Driver'",
		ParameterOIDs: nil,
	}).Encode(nil)
	buffer = (&pgproto3.Bind{
		DestinationPortal:    "",
		PreparedStatement:    "",
		ParameterFormatCodes: nil,
		Parameters:           nil,
		ResultFormatCodes:    nil,
	}).Encode(buffer)
	buffer = (&pgproto3.Execute{
		Portal:  "",
		MaxRows: 0,
	}).Encode(buffer)
	buffer = (&pgproto3.Sync{}).Encode(buffer)
	_, err = conn.Write(buffer) //set JDBC
	if err != nil {
		t.Error("出错了")
	}

	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ParseComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.BindComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.CommandComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ReadyForQuery{}, msg)

	buffer = (&pgproto3.Parse{
		Name:          "",
		Query:         "insert into t values (1)",
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

	c, err = reader.Read(buff)
	buf = buff[:c]
	re := bytes.NewReader(buf)
	msgs, err = UnpackSqliTransmission(re)
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
	done = &SqliDone{
		Warning:  0,
		Rows:     0,
		RowID:    0,
		SerialID: 0,
	}
	cost = &SqliCost{
		EstimatedRows: 1,
		EstimatedIO:   2,
	}
	transmission = []SqliCommand{describe, done, cost, eott}
	bufff, err := transmission.Pack()
	assert.Nil(err)
	_, err = conntion.Write(bufff)

	c, err = reader.Read(buff)
	assert.Nil(err)
	assert.True(c > 0)
	buf = buff[:c]
	readseeker = bytes.NewReader(buf)
	msgs, err = UnpackSqliTransmission(readseeker)
	assert.IsType(&SqliID{}, msgs[0])
	assert.IsType(&SqliCIdescribe{}, msgs[1])
	assert.IsType(&SqliEot{}, msgs[2])

	idescribe := &SqliIdescribe{
		Inputfields: 2,
		Fields: []Sqlifields{{
			Type:                 2,
			ExtendID:             0,
			OwnerNameLength:      0,
			ExtendTypeNameLength: 0,
			PassByReferenceFlag:  0,
			Alignment:            0,
			SourceType:           0,
			Length:               4,
		}, {
			Type:                 2,
			ExtendID:             0,
			OwnerNameLength:      0,
			ExtendTypeNameLength: 0,
			PassByReferenceFlag:  0,
			Alignment:            0,
			SourceType:           0,
			Length:               4,
		}},
	}
	transmission = []SqliCommand{idescribe, eott}
	buffer, err = transmission.Pack()
	_, err = conntion.Write(buffer)

	c, err = reader.Read(buff)
	assert.Nil(err)
	assert.True(c > 0)
	buf = buff[:c]
	readseeker = bytes.NewReader(buf)
	msgs, err = UnpackSqliTransmission(readseeker)
	assert.IsType(&SqliID{}, msgs[0])
	//assert.IsType(&SqliBind{}, msgs[1])
	assert.IsType(&SqliExecute{}, msgs[1])
	assert.IsType(&SqliEot{}, msgs[2])

	insertdone := &SqliInsertDone{
		Serial8:   1,
		BigSerial: 2,
	}
	done = &SqliDone{
		Warning:  0,
		Rows:     0,
		RowID:    0,
		SerialID: 0,
	}
	cost = &SqliCost{
		EstimatedRows: 1,
		EstimatedIO:   2,
	}
	transmission = []SqliCommand{insertdone, done, cost, eott}
	buffer, err = transmission.Pack()
	assert.Nil(err)
	_, err = conntion.Write(buffer)
	assert.Nil(err)

	c, err = reader.Read(buff)
	assert.Nil(err)
	assert.True(c > 0)
	buf = buff[:c]
	readseeker = bytes.NewReader(buf)
	msgs, err = UnpackSqliTransmission(readseeker)
	assert.IsType(&SqliID{}, msgs[0])
	assert.IsType(&SqliRelease{}, msgs[1])
	assert.IsType(&SqliEot{}, msgs[2])

	transmission = []SqliCommand{eott}
	buffer, err = transmission.Pack()
	assert.Nil(err)
	_, err = conntion.Write(buffer)
	assert.Nil(err)

	front = pgproto3.NewFrontend(conn, nil)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ParseComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.BindComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.NoData{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.CommandComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ReadyForQuery{}, msg)
}

func TestBridge_NonOpen_Bind(t *testing.T) {
	address := "127.0.0.1:11030"
	clientConn, err := net.ResolveTCPAddr("tcp4", address)
	assert.Nil(t, err)
	listener, err := net.ListenTCP("tcp4", clientConn)
	assert.Nil(t, err)
	defer listener.Close()

	assert := assert.New(t)
	conn, err := net.Dial("tcp4", "127.0.0.1:11088")
	assert.Nil(err)
	//front := pgproto3.NewFrontend(conn, nil)
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
	assert.Nil(err)
	assert.True(c > 0)
	buf := buff[:c]
	readseeker := bytes.NewReader(buf)
	authrequest := &AuthRequest{}
	err = authrequest.Unpack(readseeker)

	response, err := (&AuthResponse{
		Length:           287,
		Noname1:          2,
		Noname2:          15376,
		Noname3:          0,
		Noname4:          100,
		Noname5:          101,
		Noname6:          61,
		IEEEIlength:      6,
		IEEEI:            "IEEEI",
		Noname7:          108,
		Srvinfx:          "lsrvinfx",
		Versionlength:    34,
		Version:          "GBase Server Version 9.56.FC4G1TL",
		Softwarelength:   35,
		Software:         "Software Serial Number AAA#B000000",
		Clientnamelength: 12,
		Clientname:       "gbaseserver",
		Noname8:          316,
		Noname9:          0,
		Noname10:         0,
		Noname11:         0,
		Noname12:         0,
		Noname13:         0,
		Noname14:         "on",
		Noname15:         "soctcp",
		Noname16:         102,
		Noname17:         0,
		Noname18:         0,
		Noname19:         20,
		Noname20:         0,
		Noname21:         107,
		Noname22:         3785,
		Noname23:         872,
		Noname24:         13312,
		Path1length:      11,
		Path1:            "/dev/pts/0",
		Path2length:      15,
		Path2:            "/home/gbasedbt",
		Noname25:         110,
		Noname26:         4,
		Noname27:         0,
		Noname28:         0,
		Noname29:         116,
		Noname30:         43,
		Noname31:         0,
		Noname32:         1001,
		Noname33:         0,
		Noname34:         1001,
		Path3length:      21,
		Path3:            "/home/zhangyaru/gbase/bin/oninit",
		Asceot:           127,
	}).Pack()
	assert.Nil(err)
	_, err = conntion.Write(response)
	assert.Nil(err)

	c, err = reader.Read(buff)
	assert.Nil(err)
	assert.True(c > 0)
	buf = buff[:c]
	readseeker = bytes.NewReader(buf)
	msgs, err := UnpackSqliTransmission(readseeker)
	assert.IsType(&SqliProtocols{}, msgs[0])
	assert.IsType(&SqliEot{}, msgs[1])

	protocol := []byte{0, 126, 0, 9, 189, 190, 159, 254, 127, 183, 255, 239, 240, 0}
	eot, err := (&SqliEot{}).Pack()
	assert.Nil(err)
	for _, c := range eot {
		protocol = append(protocol, c)
	}
	_, err = conntion.Write(protocol)
	assert.Nil(err)

	c, err = reader.Read(buff)
	assert.Nil(err)
	assert.True(c > 0)
	buf = buff[:c]
	readseeker = bytes.NewReader(buf)
	msgs, err = UnpackSqliTransmission(readseeker)
	assert.IsType(&SqliInfo{}, msgs[0])
	assert.IsType(&SqliEot{}, msgs[1])

	_, err = conntion.Write(eot)
	assert.Nil(err)

	c, err = reader.Read(buff)
	assert.Nil(err)
	assert.True(c > 0)
	buf = buff[:c]
	readseeker = bytes.NewReader(buf)
	msgs, err = UnpackSqliTransmission(readseeker)
	assert.IsType(&SqliDBOpen{}, msgs[0])
	assert.IsType(&SqliEot{}, msgs[1])

	done := &SqliDone{
		Warning:  21,
		Rows:     0,
		RowID:    0,
		SerialID: 0,
	}
	cost := &SqliCost{
		EstimatedRows: 1,
		EstimatedIO:   1,
	}
	eott := &SqliEot{}
	var transmission SqliTransmission
	transmission = []SqliCommand{done, cost, eott}
	buf, err = transmission.Pack()
	_, err = conntion.Write(buf)
	assert.Nil(err)

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
		Query:         "SET extra_float_digits = 3",
		ParameterOIDs: nil,
	}).Encode(nil)
	buffer = (&pgproto3.Bind{
		DestinationPortal:    "",
		PreparedStatement:    "",
		ParameterFormatCodes: nil,
		Parameters:           nil,
		ResultFormatCodes:    nil,
	}).Encode(buffer)
	buffer = (&pgproto3.Execute{
		Portal:  "",
		MaxRows: 1,
	}).Encode(buffer)
	buffer = (&pgproto3.Sync{}).Encode(buffer)
	_, err = conn.Write(buffer) //set digits
	if err != nil {
		t.Error("出错了")
	}

	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ParseComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.BindComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.CommandComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ReadyForQuery{}, msg)

	buffer = (&pgproto3.Parse{
		Name:          "",
		Query:         "SET application_name = 'PostgreSQL JDBC Driver'",
		ParameterOIDs: nil,
	}).Encode(nil)
	buffer = (&pgproto3.Bind{
		DestinationPortal:    "",
		PreparedStatement:    "",
		ParameterFormatCodes: nil,
		Parameters:           nil,
		ResultFormatCodes:    nil,
	}).Encode(buffer)
	buffer = (&pgproto3.Execute{
		Portal:  "",
		MaxRows: 0,
	}).Encode(buffer)
	buffer = (&pgproto3.Sync{}).Encode(buffer)
	_, err = conn.Write(buffer) //set JDBC
	if err != nil {
		t.Error("出错了")
	}

	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ParseComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.BindComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.CommandComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ReadyForQuery{}, msg)

	buffer = (&pgproto3.Parse{
		Name:          "",
		Query:         "insert into t values (1)",
		ParameterOIDs: nil,
	}).Encode(nil)
	buffer = (&pgproto3.Bind{
		DestinationPortal:    "",
		PreparedStatement:    "",
		ParameterFormatCodes: []int16{1},
		Parameters:           [][]byte{{2}},
		ResultFormatCodes:    []int16{0},
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

	c, err = reader.Read(buff)
	buf = buff[:c]
	re := bytes.NewReader(buf)
	msgs, err = UnpackSqliTransmission(re)
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
	done = &SqliDone{
		Warning:  0,
		Rows:     0,
		RowID:    0,
		SerialID: 0,
	}
	cost = &SqliCost{
		EstimatedRows: 1,
		EstimatedIO:   2,
	}
	transmission = []SqliCommand{describe, done, cost, eott}
	bufff, err := transmission.Pack()
	assert.Nil(err)
	_, err = conntion.Write(bufff)

	c, err = reader.Read(buff)
	assert.Nil(err)
	assert.True(c > 0)
	buf = buff[:c]
	readseeker = bytes.NewReader(buf)
	msgs, err = UnpackSqliTransmission(readseeker)
	assert.IsType(&SqliID{}, msgs[0])
	assert.IsType(&SqliCIdescribe{}, msgs[1])
	assert.IsType(&SqliEot{}, msgs[2])

	idescribe := &SqliIdescribe{
		Inputfields: 2,
		Fields: []Sqlifields{{
			Type:                 2,
			ExtendID:             0,
			OwnerNameLength:      0,
			ExtendTypeNameLength: 0,
			PassByReferenceFlag:  0,
			Alignment:            0,
			SourceType:           0,
			Length:               4,
		}, {
			Type:                 2,
			ExtendID:             0,
			OwnerNameLength:      0,
			ExtendTypeNameLength: 0,
			PassByReferenceFlag:  0,
			Alignment:            0,
			SourceType:           0,
			Length:               4,
		}},
	}
	transmission = []SqliCommand{idescribe, eott}
	buffer, err = transmission.Pack()
	_, err = conntion.Write(buffer)

	c, err = reader.Read(buff)
	assert.Nil(err)
	assert.True(c > 0)
	buf = buff[:c]
	readseeker = bytes.NewReader(buf)
	msgs, err = UnpackSqliTransmission(readseeker)
	assert.IsType(&SqliID{}, msgs[0])
	assert.IsType(&SqliBind{}, msgs[1])
	assert.IsType(&SqliExecute{}, msgs[2])
	assert.IsType(&SqliEot{}, msgs[3])

	insertdone := &SqliInsertDone{
		Serial8:   1,
		BigSerial: 2,
	}
	done = &SqliDone{
		Warning:  0,
		Rows:     0,
		RowID:    0,
		SerialID: 0,
	}
	cost = &SqliCost{
		EstimatedRows: 1,
		EstimatedIO:   2,
	}
	transmission = []SqliCommand{insertdone, done, cost, eott}
	buffer, err = transmission.Pack()
	assert.Nil(err)
	_, err = conntion.Write(buffer)
	assert.Nil(err)

	c, err = reader.Read(buff)
	assert.Nil(err)
	assert.True(c > 0)
	buf = buff[:c]
	readseeker = bytes.NewReader(buf)
	msgs, err = UnpackSqliTransmission(readseeker)
	assert.IsType(&SqliID{}, msgs[0])
	assert.IsType(&SqliRelease{}, msgs[1])
	assert.IsType(&SqliEot{}, msgs[2])

	transmission = []SqliCommand{eott}
	buffer, err = transmission.Pack()
	assert.Nil(err)
	_, err = conntion.Write(buffer)
	assert.Nil(err)

	front = pgproto3.NewFrontend(conn, nil)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ParseComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.BindComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.NoData{}, msg)
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

func TestConn(t *testing.T) {
	address := "127.0.0.1:11030"
	clientConn, err := net.ResolveTCPAddr("tcp4", address)
	assert.Nil(t, err)
	listener, err := net.ListenTCP("tcp4", clientConn)
	assert.Nil(t, err)
	defer listener.Close()

	assert := assert.New(t)
	conn, err := net.Dial("tcp4", "127.0.0.1:11088") //conn : 11088
	assert.Nil(err)
	front := pgproto3.NewFrontend(conn, nil)
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
	_, err = conn.Write(start) //startup message
	if err != nil {
		t.Error("出错了")
	}

	conntion, err := listener.Accept() //conntion : 11030
	assert.Nil(err)
	reader := NewReader(conntion)
	buff := make([]byte, 16384)
	c, err := reader.Read(buff) //authrequest
	assert.Nil(err)
	assert.True(c > 0)
	buf := buff[:c]
	readseeker := bytes.NewReader(buf)
	authrequest := &AuthRequest{}
	err = authrequest.Unpack(readseeker)

	response, err := (&AuthResponse{
		Length:           287,
		Noname1:          2,
		Noname2:          15376,
		Noname3:          0,
		Noname4:          100,
		Noname5:          101,
		Noname6:          61,
		IEEEIlength:      6,
		IEEEI:            "IEEEI",
		Noname7:          108,
		Srvinfx:          "lsrvinfx",
		Versionlength:    34,
		Version:          "GBase Server Version 9.56.FC4G1TL",
		Softwarelength:   35,
		Software:         "Software Serial Number AAA#B000000",
		Clientnamelength: 12,
		Clientname:       "gbaseserver",
		Noname8:          316,
		Noname9:          0,
		Noname10:         0,
		Noname11:         0,
		Noname12:         0,
		Noname13:         0,
		Noname14:         "on",
		Noname15:         "soctcp",
		Noname16:         102,
		Noname17:         0,
		Noname18:         0,
		Noname19:         20,
		Noname20:         0,
		Noname21:         107,
		Noname22:         3785,
		Noname23:         872,
		Noname24:         13312,
		Path1length:      11,
		Path1:            "/dev/pts/0",
		Path2length:      15,
		Path2:            "/home/gbasedbt",
		Noname25:         110,
		Noname26:         4,
		Noname27:         0,
		Noname28:         0,
		Noname29:         116,
		Noname30:         43,
		Noname31:         0,
		Noname32:         1001,
		Noname33:         0,
		Noname34:         1001,
		Path3length:      21,
		Path3:            "/home/zhangyaru/gbase/bin/oninit",
		Asceot:           127,
	}).Pack()
	assert.Nil(err)
	conntion.Write(response) //authresponse

	c, err = reader.Read(buff) //sqliprotocol
	assert.Nil(err)
	assert.True(c > 0)
	buf = buff[:c]
	readseeker = bytes.NewReader(buf)
	msgs, err := UnpackSqliTransmission(readseeker)
	assert.IsType(&SqliProtocols{}, msgs[0])
	assert.IsType(&SqliEot{}, msgs[1])

	protocol := []byte{0, 126, 0, 9, 189, 190, 159, 254, 127, 183, 255, 239, 240, 0}
	eot, err := (&SqliEot{}).Pack()
	assert.Nil(err)
	for _, c := range eot {
		protocol = append(protocol, c)
	}
	_, err = conntion.Write(protocol) //sqliprotocol
	assert.Nil(err)

	c, err = reader.Read(buff) //sqliinfo
	assert.Nil(err)
	assert.True(c > 0)
	buf = buff[:c]
	readseeker = bytes.NewReader(buf)
	msgs, err = UnpackSqliTransmission(readseeker)
	assert.IsType(&SqliInfo{}, msgs[0])
	assert.IsType(&SqliEot{}, msgs[1])

	_, err = conntion.Write(eot) //sqlieot
	assert.Nil(err)

	c, err = reader.Read(buff) //sqlidbopen
	assert.Nil(err)
	assert.True(c > 0)
	buf = buff[:c]
	readseeker = bytes.NewReader(buf)
	msgs, err = UnpackSqliTransmission(readseeker)
	assert.IsType(&SqliDBOpen{}, msgs[0])
	assert.IsType(&SqliEot{}, msgs[1])

	done := &SqliDone{
		Warning:  21,
		Rows:     0,
		RowID:    0,
		SerialID: 0,
	}
	cost := &SqliCost{
		EstimatedRows: 1,
		EstimatedIO:   1,
	}
	eott := &SqliEot{}
	var transmission SqliTransmission
	transmission = []SqliCommand{done, cost, eott}
	buf, err = transmission.Pack()
	_, err = conntion.Write(buf) //sqlidone
	assert.Nil(err)

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
		Query:         "SET extra_float_digits = 3",
		ParameterOIDs: nil,
	}).Encode(nil)
	buffer = (&pgproto3.Bind{
		DestinationPortal:    "",
		PreparedStatement:    "",
		ParameterFormatCodes: nil,
		Parameters:           nil,
		ResultFormatCodes:    nil,
	}).Encode(buffer)
	buffer = (&pgproto3.Execute{
		Portal:  "",
		MaxRows: 1,
	}).Encode(buffer)
	buffer = (&pgproto3.Sync{}).Encode(buffer)
	_, err = conn.Write(buffer) //set digits
	if err != nil {
		t.Error("出错了")
	}

	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ParseComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.BindComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.CommandComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ReadyForQuery{}, msg)

	buffer = (&pgproto3.Parse{
		Name:          "",
		Query:         "SET application_name = 'PostgreSQL JDBC Driver'",
		ParameterOIDs: nil,
	}).Encode(nil)
	buffer = (&pgproto3.Bind{
		DestinationPortal:    "",
		PreparedStatement:    "",
		ParameterFormatCodes: nil,
		Parameters:           nil,
		ResultFormatCodes:    nil,
	}).Encode(buffer)
	buffer = (&pgproto3.Execute{
		Portal:  "",
		MaxRows: 0,
	}).Encode(buffer)
	buffer = (&pgproto3.Sync{}).Encode(buffer)
	_, err = conn.Write(buffer) //set JDBC
	if err != nil {
		t.Error("出错了")
	}

	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ParseComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.BindComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.CommandComplete{}, msg)
	msg, err = front.Receive()
	assert.Nil(err)
	assert.IsType(&pgproto3.ReadyForQuery{}, msg)
}
