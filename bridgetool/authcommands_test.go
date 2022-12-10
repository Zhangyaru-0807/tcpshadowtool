package bridgetoolpackage

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAuthRequest_Pack(t *testing.T) {
	authrequest := &AuthRequest{
		Length:           433,
		Noname1:          1,
		Noname2:          60,
		Noname3:          0,
		Noname4:          100,
		Noname5:          101,
		Noname6:          61,
		Ieeemlength:      6,
		Ieeem:            "IEEEM",
		Noname7:          108,
		Sqlexec:          "sqlexec",
		Versionlength:    6,
		Version:          "9.280",
		Numberlength:     12,
		Rds:              "RDS#R000000",
		Sqlilength:       5,
		Sqli:             "sqli",
		Noname8:          316,
		Noname9:          0,
		Noname10:         0,
		Noname11:         1,
		Clientnamelength: 9,
		Clientname:       "gbasedbt",
		Passwordlength:   33,
		Password:         "HmQOYC1ZfTYt+vlXUhkn3w==",
		Noname12:         "ol",
		Noname13:         61,
		Tlitcp:           "tlitcp",
		Noname14:         1,
		Noname15:         104,
		Asf:              11,
		Noname16:         3,
		Servernamelength: 12,
		Servername:       "gbaseserver",
		Noname17:         0,
		Noname18:         0,
		Noname19:         0,
		Noname20:         0,
		Noname21:         0,
		Noname22:         106,
		Noname23:         6,
		Dpath: []DPath{{
			Dbpathlength:          7,
			Dbpath:                "DBPATH",
			Dbpathattributelength: 2,
			Dbpathattribute:       ".",
		}, {
			Dbpathlength:          17,
			Dbpath:                "CLNT_PAM_CAPABLE",
			Dbpathattributelength: 2,
			Dbpathattribute:       "1",
		}, {
			Dbpathlength:          7,
			Dbpath:                "DBDATE",
			Dbpathattributelength: 6,
			Dbpathattribute:       "Y4MD-",
		}, {
			Dbpathlength:          12,
			Dbpath:                "IFX_UPDDESC",
			Dbpathattributelength: 2,
			Dbpathattribute:       "1",
		}, {
			Dbpathlength:          8,
			Dbpath:                "SQLMODE",
			Dbpathattributelength: 6,
			Dbpathattribute:       "gbase",
		}, {
			Dbpathlength:          9,
			Dbpath:                "NODEFDAC",
			Dbpathattributelength: 3,
			Dbpathattribute:       "no",
		}},
		Noname24:         107,
		Noname25:         0,
		Noname26:         0,
		Longthreadid:     1,
		Hostnamelength:   16,
		Noname27:         "MM-202201031507",
		Noname28:         0,
		Directorylength:  21,
		Directory:        "E:\\JDBCTest\\JDBCTest",
		Noname29:         116,
		Appnamelengthall: 80,
		Noname30:         0,
		Noname31:         0,
		Appnamelength:    70,
		Appname:          "/E:/JDBCTest/JDBCTest/lib/gbasedbtjdbc_3.3.0_2.jarConnectionTest/Test",
		Asceot:           127,
	}
	pack, err := authrequest.Pack()
	assert.Nil(t, err)
	t.Log(pack)
	buffer := []byte{1, 177, 1, 60, 0, 0, 0, 100, 0, 101, 0, 0, 0, 61, 0, 6, 73, 69, 69, 69, 77, 0, 0, 108, 115, 113, 108, 101, 120, 101, 99, 0, 0, 0, 0, 0, 0, 6, 57, 46, 50, 56, 48, 0, 0, 12, 82, 68, 83, 35, 82,
		48, 48, 48, 48, 48, 48, 0, 0, 5, 115, 113, 108, 105, 0, 0, 0, 1, 60, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 9, 103, 98, 97, 115, 101, 100, 98, 116, 0, 0, 33, 1, 1, 1, 1, 1, 1, 1, 1, 72, 109, 81, 79, 89, 67, 49, 90,
		102, 84, 89, 116, 43, 118, 108, 88, 85, 104, 107, 110, 51, 119, 61, 61, 0, 111, 108, 0, 0, 0, 0, 0, 0, 0, 0, 0, 61, 116, 108, 105, 116, 99, 112, 0, 0, 0, 0, 0, 1, 0, 104, 0, 11, 0, 0, 0, 3, 0, 12, 103,
		98, 97, 115, 101, 115, 101, 114, 118, 101, 114, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 106, 0, 6, 0, 7, 68, 66, 80, 65, 84, 72, 0, 0, 2, 46, 0, 0, 17, 67, 76, 78, 84, 95, 80, 65, 77, 95, 67, 65, 80, 65, 66, 76,
		69, 0, 0, 2, 49, 0, 0, 7, 68, 66, 68, 65, 84, 69, 0, 0, 6, 89, 52, 77, 68, 45, 0, 0, 12, 73, 70, 88, 95, 85, 80, 68, 68, 69, 83, 67, 0, 0, 2, 49, 0, 0, 8, 83, 81, 76, 77, 79, 68, 69, 0, 0, 6, 103, 98, 97,
		115, 101, 0, 0, 9, 78, 79, 68, 69, 70, 68, 65, 67, 0, 0, 3, 110, 111, 0, 0, 107, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 16, 77, 77, 45, 50, 48, 50, 50, 48, 49, 48, 51, 49, 53, 48, 55, 0, 0, 0, 0, 21, 69, 58, 92,
		74, 68, 66, 67, 84, 101, 115, 116, 92, 74, 68, 66, 67, 84, 101, 115, 116, 0, 0, 116, 0, 80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 70, 47, 69, 58, 47, 74, 68, 66, 67, 84, 101, 115, 116, 47, 74, 68, 66, 67, 84, 101,
		115, 116, 47, 108, 105, 98, 47, 103, 98, 97, 115, 101, 100, 98, 116, 106, 100, 98, 99, 95, 51, 46, 51, 46, 48, 95, 50, 46, 106, 97, 114, 67, 111, 110, 110, 101, 99, 116, 105, 111, 110, 84, 101, 115, 116,
		47, 84, 101, 115, 116, 0, 0, 127}
	assert.Equal(t, pack, buffer)

}
