package nsq

import (
	. "activeuser/envbuild"
	. "activeuser/logs"
	"errors"
	"fmt"
	"github.com/bitly/go-nsq"
	"log"
	//"net"
	"os"
	"time"
)

type Handle struct {
	msgchan chan *nsq.Message
	stop    bool
}

func (h *Handle) HandleMsg(m *nsq.Message) error {
	if !h.stop {
		h.msgchan <- m
	}
	return nil
}

func (h *Handle) Process() {

	h.stop = false
	for {
		select {
		case m := <-h.msgchan:
			err := Decode(string(m.Body))
			if err != nil {
				Logger.Critical(err)
			}
		case <-time.After(time.Hour):
			if h.stop {
				close(h.msgchan)
				return
			}
		}
	}
}

func (h *Handle) Stop() {
	h.stop = true
}

var consumer *nsq.Consumer
var err error
var h *Handle
var Upload_chan chan string
var config *nsq.Config
var logger *log.Logger

func NewConsummer(topic, channel string) error {

	envconf := GetEnvConf()
	if envconf == nil {

		return errors.New("GetEnvConf() 配置为空，尚未初始化")
	}

	config = nsq.NewConfig()
	//心跳间隔时间 3s
	config.HeartbeatInterval = 3000000000
	//1分钟去发现一次，发现topic为指定的nsqd
	config.LookupdPollInterval = 60000000000
	//config.LocalAddr, _ = net.ResolveTCPAddr("tcp", envconf.Consumerip+":"+envconf.Consumerport)

	println("HeartbeatInterval", config.HeartbeatInterval)
	println("MaxAttempts", config.MaxAttempts)
	println("LookupdPollInterval", config.LookupdPollInterval)
	//println("Consumer IPAddress", config.LocalAddr.String())

	logfile, err := os.OpenFile("../../log/au_consumer.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("%s\r\n", err.Error())
		os.Exit(-1)
	}

	//defer logfile.Close()
	logger = log.New(logfile, "\r\n", log.Ldate|log.Ltime|log.Llongfile)
	logger = log.New(os.Stdin, "", log.LstdFlags)

	consumer, err = nsq.NewConsumer(topic, channel, config)
	if err != nil {
		return err
	}
	consumer.SetLogger(logger, nsq.LogLevelInfo)
	return nil
}

func ConsumerRun(address string) error {

	h = new(Handle)
	consumer.AddHandler(nsq.HandlerFunc(h.HandleMsg))
	h.msgchan = make(chan *nsq.Message, 1024)

	//fmt.Println("Consumer address ", address)
	err = consumer.ConnectToNSQLookupd(address)
	//err = consumer.ConnectToNSQD(address)
	if err != nil {
		return err
	}
	h.Process()
	return nil
}
