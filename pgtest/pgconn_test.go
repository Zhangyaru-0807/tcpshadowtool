package main

import "testing"

func TestPgConnection(t *testing.T) {
	//连接数据库
	conn, err := db_connect()
	if err != nil {
		t.Errorf("error连接数据库")
	} //程序运行结束时关闭连接
	conn.Close()
}

func TestPgCreatetb(t *testing.T) {
	conn, err := db_connect()
	if err != nil {
		t.Errorf("error连接数据库")
	} //程序运行结束时关闭连接
	ceateTable(conn, err)
	conn.Close()
}
