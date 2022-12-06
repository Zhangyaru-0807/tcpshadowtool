package bridgetoolpackage

import (
	"bytes"
	"encoding/binary"
	"github.com/zhuangsirui/binpacker"
	"io"
)

type AuthCommand interface {
	Pack() ([]byte, error)
	Unpack(r io.Reader) error
}
type AuthRequest struct {
	header []Header
	body   []Body
}

func (au *AuthRequest) Pack() ([]byte, error) {
	buffer := new(bytes.Buffer)
	packer := binpacker.NewPacker(binary.BigEndian, buffer)
	packer.PushUint16(uint16(len(au.header) + len(au.body)))
	for _, c := range au.header {
		packer.PushUint8(c.noname1).
			PushUint8(c.noname2).
			PushUint16(c.noname3)
	}
	for _, b := range au.body {
		packer.PushUint16(b.noname1).
			PushUint16(b.noname2).
			PushUint32(b.noname3).
			PushInt16(int16(b.ieeemlength)).
			PushBytes([]byte(b.ieeem)).
			PushUint8(b.non).
			PushInt16(int16(b.noname4)).
			PushBytes([]byte(b.sqlexec)).
			PushUint16(b.versionlength).
			PushBytes([]byte(b.version)).
			PushUint8(b.noname5).
			PushInt16(int16(b.numberlength)).
			PushUint8(b.rds).
			PushUint8(b.noname6).
			PushUint16(b.sqlilength).
			PushBytes([]byte(b.sqli)).
			PushUint8(b.noname7).
			PushUint32(b.noname8).
			PushUint32(b.noname9).
			PushUint32(b.noname10).
			PushUint16(b.noname11).
			PushUint16(b.clientnamelength).
			PushBytes([]byte(b.clientname)).
			PushUint8(b.noname12).
			PushUint16(b.passwordlength).
			PushBytes([]byte(b.password)).
			PushUint8(b.noname13).
			PushBytes([]byte(b.noname14)).
			PushUint32(b.noname15).
			PushBytes([]byte(b.tlitcp)).
			PushUint32(b.noname16).
			PushUint16(b.noname17).
			PushUint16(b.asf).
			PushUint32(b.noname18).
			PushUint16(b.servernamelength).
			PushBytes([]byte(b.servername)).
			PushUint8(b.noname19).
			PushUint16(b.noname20).
			PushUint16(b.noname21).
			PushUint16(b.noname22).
			PushUint16(b.noname23).
			PushUint16(b.noname24).
			PushUint16(b.noname25).
			PushUint16(b.noname26).
			PushUint16(b.dbpathlength).
			PushBytes([]byte(b.dbpath)).
			PushUint8(b.noname27).
			PushUint16(b.dbpathattributelength).
			PushBytes([]byte(b.dbpathattribute)).
			PushUint8(b.noname28).
			PushUint16(b.noname29).
			PushUint32(b.noname30).
			PushUint32(b.noname31).
			PushUint16(b.hostnamelength).
			PushBytes([]byte(b.noname32)).
			PushUint8(b.noname33).
			PushUint16(b.noname34).
			PushUint16(b.directorylength).
			PushBytes([]byte(b.directory)).
			PushUint8(b.noname35).
			PushUint16(b.noname36).
			PushUint16(b.appnamelengthall).
			PushUint32(b.noname37).
			PushUint32(b.noname38).
			PushUint16(b.appnamelength).
			PushBytes([]byte(b.appname)).
			PushUint8(b.noname39).
			PushUint16(b.asceot)
	}

	return buffer.Bytes(), packer.Error()
}

type Header struct {
	Length  uint16
	noname1 uint8
	noname2 uint8
	noname3 uint16
}
type Body struct {
	noname1               uint16
	noname2               uint16
	noname3               uint32
	ieeemlength           uint16
	ieeem                 string
	non                   uint8
	noname4               uint16
	sqlexec               string
	versionlength         uint16
	version               string
	noname5               uint8
	numberlength          uint16
	rds                   uint8
	noname6               uint8
	sqlilength            uint16
	sqli                  string
	noname7               uint8
	noname8               uint32
	noname9               uint32
	noname10              uint32
	noname11              uint16
	clientnamelength      uint16
	clientname            string
	noname12              uint8
	passwordlength        uint16
	password              string
	noname13              uint8
	noname14              string
	noname15              uint32
	tlitcp                string
	noname16              uint32
	noname17              uint16
	asf                   uint16
	noname18              uint32
	servernamelength      uint16
	servername            string
	noname19              uint8
	noname20              uint16
	noname21              uint16
	noname22              uint16
	noname23              uint16
	noname24              uint16
	noname25              uint16
	noname26              uint16
	dbpathlength          uint16
	dbpath                string
	noname27              uint8
	dbpathattributelength uint16
	dbpathattribute       string
	noname28              uint8
	noname29              uint16
	noname30              uint32
	noname31              uint32
	longthreadid          uint32
	hostnamelength        uint16
	noname32              string
	noname33              uint8
	noname34              uint16
	directorylength       uint16
	directory             string
	noname35              uint8
	noname36              uint16
	appnamelengthall      uint16
	noname37              uint32
	noname38              uint32
	appnamelength         uint16
	appname               string
	noname39              uint8
	asceot                uint16
}

//request
