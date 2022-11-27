package main

import (
	"fmt"
	"github.com/jackc/pgx"
	"strings"
	"time"
)

func main() {
	var error_msg string

	//连接数据库
	conn, err := db_connect()
	if err != nil {
		error_msg = "连接数据库失败，详情：" + err.Error()
		write_log("Error", error_msg)
		return
	} //程序运行结束时关闭连接
	defer conn.Close()
	write_log("Log", "连接数据库成功")
	//ceateTable(conn, err)
	insertLine(conn, err)
}

/*
功能描述：插入行
参数说明：
conn *pgx.Conn -- 连接信息
err error --错误信息
返回值说明：无
*/
func insertLine(conn *pgx.Conn, err error) {
	var error_msg string
	var sql string
	var nickname string
	condition1 := false
	condition2 := false
	condition3 := false

	//插入数据
	if condition1 {
		sql = "insert into pgtest values('1','zhangsan'),('2','lisi');"
		_, err = conn.Exec(sql)
		if err != nil {
			error_msg = "插入数据失败,详情：" + err.Error()
			write_log("Error", error_msg)
			return
		} else {
			write_log("Log", "插入数据成功")
		}
	}

	//绑定变量插入数据,不需要做防注入处理
	if condition2 {
		sql = "insert into pgtest values($1,$2),($3,$4);"
		_, err = conn.Exec(sql, "3", "postgresql", "4", "postgres")
		if err != nil {
			error_msg = "插入数据失败,详情：" + err.Error()
			write_log("Error", error_msg)
			return
		} else {
			write_log("Log", "插入数据成功")
		}
	}

	//拼接sql 语句插入数据,需要做防注入处理
	if condition3 {
		nickname = "pg is good!"
		sql = "insert into pgtest values('1','" + sql_data_encode(nickname) + "')"
		_, err = conn.Exec(sql)
		if err != nil {
			error_msg = "插入数据失败,详情：" + err.Error()
			write_log("Error", error_msg)
			return
		} else {
			write_log("Log", "插入数据成功")
		}
	}
}

// 替换字符串，n为替换次数，负数表示无限制
func sql_data_encode(str string) string {
	return strings.Replace(str, "pg", "sqli", -1)
}

/*
功能描述：创建表
参数说明：
conn *pgx.Conn -- 连接信息
err error --错误信息
返回值说明：无
*/
func ceateTable(conn *pgx.Conn, err error) {
	var sql string
	var error_msg string

	sql = "create table pgtest1(id varchar(20), name varchar(100));"
	_, err = conn.Exec(sql)
	if err != nil {
		error_msg = "创建数据表失败,详情：" + err.Error()
		write_log("Error", error_msg)
		return
	} else {
		write_log("Log", "创建数据表成功")
	}
}

/*
功能描述：写入日志处理
参数说明：
log_level -- 日志级别，只能是 Error 或 Log
error_msg -- 日志内容
返回值说明：无
*/
func write_log(log_level string, error_msg string) {
	//打印错误信息
	fmt.Println("访问时间：", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Println("日志级别：", log_level)
	fmt.Println("详细信息：", error_msg)
}

/*
功能描述：连接数据库
参数说明：无
返回值说明：
conn *pgx.Conn -- 连接信息
err error --错误信息
*/
func db_connect() (conn *pgx.Conn, err error) {
	var config pgx.ConnConfig
	config.Host = "127.0.0.1"    //数据库主机 host 或 IP
	config.User = "postgres"     //连接用户
	config.Password = "111111"   //用户密码
	config.Database = "postgres" //连接数据库名
	config.Port = 5432           //端口号
	conn, err = pgx.Connect(config)
	return conn, err
}
