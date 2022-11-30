package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"os"
)

func main() {
	/** pg数据库链接测试**/
	urlExample := "postgres://root:111111@127.0.0.1:15432/postgres?sslmode=disable"
	conn, err := pgx.Connect(context.Background(), urlExample)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	var name string
	err = conn.QueryRow(context.Background(), "select * from test;").Scan(&name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(name)
}
