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

	var id int32
	conn, err := db_connect()
	if err != nil {
		t.Errorf("error连接数据库")
	} //程序运行结束时关闭连接
	insertLine(conn, err)
	defer conn.Close()

	rows, err := conn.Query("select id from t where id = 100")
	if err != nil {
		t.Fatalf("conn.Query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		rows.Scan(&id)

	}
	if id != 100 {
		t.Error("Select called onDataRow wrong number of times")
	}
}

func TestPgSelectLine(t *testing.T) {
	t.Parallel()
	var sum, rowCount int32
	//var name string

	//sql := "select * from t"
	conn, err := db_connect()
	if err != nil {
		t.Errorf("error连接数据库")
	} //程序运行结束时关闭连接

	//selectTable(conn, err, sql)
	defer conn.Close()

	rows, err := conn.Query("select id from t")
	if err != nil {
		t.Fatalf("conn.Query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int32
		rows.Scan(&id)
		sum += id
		rowCount++
	}
	if rowCount != 7 {
		t.Error("Select called onDataRow wrong number of times")
	}
	if sum != 29 {
		t.Error("Wrong values returned")
	}
}
