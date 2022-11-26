package main

import (
	"fmt"
	"github.com/jackc/pgx"
	"time"
)

func main() {
	var error_msg string
	var sql string

	//连接数据库
	conn, err := db_connect()
	if err != nil {
		error_msg = "连接数据库失败，详情：" + err.Error()
		write_log("Error", error_msg)
		return
	} //程序运行结束时关闭连接
	defer conn.Close()
	write_log("Log", "连接数据库成功")

	condition := false
	//建立数据表
	if condition == true {
		sql = "create table pgtest(id varchar(20), name varchar(100));"
		_, err = conn.Exec(sql)
		if err != nil {
			error_msg = "创建数据表失败,详情：" + err.Error()
			write_log("Error", error_msg)
			return
		} else {
			write_log("Log", "创建数据表成功")
		}
	}
	
	if condition == true {

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
