package socket

import (
	. "activeuser/activerule"
	. "activeuser/logs"
	"activeuser/protocol"
	"fmt"
	"net"
	"os"
	"time"
)

type Walkday struct {
	Walkdate  int64  `json:"walkdate"`
	Walkhour  string `json:"walkhour"`
	Walktotal int    `json:"walktotal"`
	Recipe    string `json:"recipe"`
}

type Walkdata struct {
	Userid    int64     `json:"userid"`
	Timestamp int64     `json:"timestamp"`
	Walkdays  []Walkday `json:"walkdays"`
}

var Msgqueue *Queue
var server = "localhost:6080"
var error_conn_chan chan int

func init() {

	Msgqueue = NewQueue()
	error_conn_chan = make(chan int, 1)
	//连接activemaster ...
	go connect()
	go reconnect()
}

func send(conn net.Conn) {

	var total int

	n, err := conn.Write(protocol.Enpack(&protocol.Message{"activeuser@xxxooo", 0}))

	if err != nil {
		total += n
		fmt.Printf("write %d bytes, error:%s\n", n, err)
		os.Exit(1)
	}
	total += n
	//fmt.Printf("write regist %d bytes this time, %d bytes in total\n", n, total)

	var total0 int

	n0, err0 := conn.Write(protocol.Enpack(&protocol.Message{"heartbeat", 1}))

	if err0 != nil {
		total0 += n0
		fmt.Printf("write %d bytes, error:%s\n", n0, err0)
		os.Exit(1)
	}
	total0 += n0
	//fmt.Printf("write heartbeat %d bytes this time, %d bytes in total\n", n0, total0)
}

func HandleRead(conn net.Conn) {

	// 缓冲区，存储被截断的数据
	tmpBuffer := make([]byte, 0)

	//接收解包
	readerChannel := make(chan protocol.Message, 1024)
	//fmt.Printf("%d connection connected into server\n", index)
	go reader(conn, readerChannel)

	buffer := make([]byte, 1024)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			Logger.Debug(conn.RemoteAddr().String(), " connection error: ", err)
			error_conn_chan <- 0
			return
		}

		tmpBuffer = protocol.Depack(append(tmpBuffer, buffer[:n]...), readerChannel)
	}
	defer conn.Close()
}

func reconnect() {

	//每5S查看一次error_conn_chan是否连接坏掉，坏掉的话重练，重连出问题，退出程序
	for {
		select {

		case <-error_conn_chan:
			connect()

		default:
			time.Sleep(5 * time.Second)

		}
	}

}

func connect() {

	tryconnecttimes := 0
	fmt.Println("connectting activemaster ... ")
	var conn net.Conn
	var err error

	for {

		conn, err = net.DialTimeout("tcp", server, time.Second*2)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Fatal error: %s\n", err.Error())
			tryconnecttimes += 1
			time.Sleep(2 * time.Second)
			//尝试重连5次,超过次数退出连接。。
			if tryconnecttimes == 5 {
				Logger.Critical("连接activemaster失败，5次重连均失败，问题很严重，系统将退出，请注意！！！", err)
				tryconnecttimes = 1
				//过10S再连
				time.Sleep(10 * time.Second)
			}
			continue

		} else {

			break
		}
	}

	fmt.Println("connect to activemaster success")
	go HandleRead(conn)
	send(conn)

}

func reader(conn net.Conn, readerChannel chan protocol.Message) {
	for {
		select {

		case data := <-readerChannel:

			switch data.MsgType {
			/*
				新来的注册client，需要Server先发送心跳包，开始双方之间的aliveCheck，同时启动SetDeadline，
				如超时未收到消息，则关闭链接
			*/
			case 0:
				fmt.Println("zero")
			//收到心跳包，重启计时；否则，短连接处理到时，会销毁conn
			case 1:
				//fmt.Println(data.MsgContent)
				conn.Write(protocol.Enpack(&protocol.Message{"heartbeat", 1}))
				time.Sleep(1 * time.Second)
				conn.SetReadDeadline(time.Now().Add(time.Duration(5) * time.Second))

			case 2:
				Decode(data.MsgContent)

			default:
				fmt.Println("weird happens")
			}

		}
	}
}
