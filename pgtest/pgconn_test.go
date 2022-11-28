package main

import (
	"testing"
)

func TestPgConnection(t *testing.T) {
	t.Parallel()

	//连接数据库
	conn, err := db_connect()
	if err != nil {
		t.Errorf("error连接数据库")
	} //程序运行结束时关闭连接
	defer conn.Close()
	
	var result int
	err = conn.QueryRow("select 1 +1").Scan(&result)
	if err != nil {
		t.Fatalf("QueryRow Scan unexpectedly failed: %v", err)
	}
	if result != 2 {
		t.Errorf("bad result: %d", result)
	}
}

func TestPgCreatetb(t *testing.T) {
	t.Parallel()

	conn, err := db_connect()
	if err != nil {
		t.Errorf("error连接数据库")
	} //程序运行结束时关闭连接
	ceateTable(conn, err)
	conn.Close()
}

func TestPgInsertLine(t *testing.T) {
	t.Parallel()

	conn, err := db_connect()
	if err != nil {
		t.Errorf("error连接数据库")
	} //程序运行结束时关闭连接
	insertLine(conn, err)
	conn.Close()
}

func TestPgSelectLine(t *testing.T) {
	t.Parallel()

	conn, err := db_connect()
	if err != nil {
		t.Errorf("error连接数据库")
	} //程序运行结束时关闭连接
	selectTable(conn, err)
	conn.Close()
}
