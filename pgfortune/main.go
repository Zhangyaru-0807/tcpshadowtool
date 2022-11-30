package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
)

var options struct {
	listenAddress   string
	responseCommand string
}

type ClientConn struct {
	rb *bufio.Reader
	c  net.Conn
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage:  %s [options]\n", os.Args[0])
		flag.PrintDefaults()
	}

	//命令行参数解析
	flag.StringVar(&options.listenAddress, "listen", "127.0.0.1:15432", "Listen address")
	flag.StringVar(&options.responseCommand, "response-command", "fortune | cowsay -f elephant", "Command to execute to generate query response")
	flag.Parse()

	//net.listen是客户端的一种监听方法
	ln, err := net.Listen("tcp", options.listenAddress)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Listening on", ln.Addr())

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		log.Println("Accepted connection from", conn.RemoteAddr())

		b := NewPgFortuneBackend(conn, func() ([]byte, error) {
			return exec.Command("sh", "-c", options.responseCommand).CombinedOutput()
		})
		go func() {
			err := b.Run()
			if err != nil {
				log.Println(err)
			}
			log.Println("Closed connection from", conn.RemoteAddr())
		}()
	}
}
