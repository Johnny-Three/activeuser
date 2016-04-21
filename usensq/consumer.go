package usensq

import (
	. "activeuser/envbuild"
	. "activeuser/logs"
	"errors"
	"fmt"
	"github.com/bitly/go-nsq"
	"log"
	//"net"
	"os"
	"strings"
	"time"
)

type Handlet struct {
	msgchan chan *nsq.Message
	stop    bool
}

func (h *Handlet) HandleMsg(m *nsq.Message) error {
	if !h.stop {
		h.msgchan <- m
	}
	return nil
}

func (h *Handlet) Process() {

	h.stop = false
	for {
		select {
		case m := <-h.msgchan:
			fmt.Println("我拿到了一条消息，打出来看看是什么：")
			fmt.Println(string(m.Body))
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

func (h *Handlet) Stop() {
	h.stop = true
}

//=====================================================================
type Handleu struct {
	msgchan chan *nsq.Message
	stop    bool
}

func (h *Handleu) HandleMsg(m *nsq.Message) error {
	if !h.stop {
		h.msgchan <- m
	}
	return nil
}

func (h *Handleu) Process() {

	h.stop = false
	for {
		select {
		case m := <-h.msgchan:
			fmt.Println("xxxxxxxxxxxxxxooooooooooooooo==========我拿到了一条消息，打出来看看是什么：")
			fmt.Println(string(m.Body))
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

func (h *Handleu) Stop() {
	h.stop = true
}

var consumert *nsq.Consumer
var consumeru *nsq.Consumer
var err error
var ht *Handlet
var hu *Handleu
var Upload_chan chan string
var config *nsq.Config
var logger *log.Logger

func NewConsummer(topic, channel string) (*nsq.Consumer, error) {

	envconf := GetEnvConf()
	if envconf == nil {

		return nil, errors.New("GetEnvConf() 配置为空，尚未初始化")
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

	var consumer *nsq.Consumer
	if true == strings.EqualFold(topic, "base_data_upload") {

		consumeru, err = nsq.NewConsumer(topic, channel, config)
		if err != nil {
			return nil, err
		}
		consumeru.SetLogger(logger, nsq.LogLevelInfo)
		consumer = consumeru

	} else if true == strings.EqualFold(topic, "task_to_au") {

		consumert, err = nsq.NewConsumer(topic, channel, config)
		if err != nil {
			return nil, err
		}
		consumert.SetLogger(logger, nsq.LogLevelInfo)
		consumer = consumert
	}

	return consumer, nil
}

func ConsumerRun(consumer *nsq.Consumer, topic, address string) error {

	fmt.Println("consumer is", consumer)

	if consumer == nil {
		return errors.New("consumer尚未初始化 ")
	}

	if topic == "task_to_au" {

		ht = new(Handlet)
		consumer.AddHandler(nsq.HandlerFunc(ht.HandleMsg))
		ht.msgchan = make(chan *nsq.Message, 1024)
		//fmt.Println("Consumer address ", address)
		err = consumer.ConnectToNSQLookupd(address)
		//err = consumer.ConnectToNSQD(address)
		if err != nil {
			return err
		}
		ht.Process()
	}

	if topic == "base_data_upload" {
		hu = new(Handleu)
		consumer.AddHandler(nsq.HandlerFunc(hu.HandleMsg))
		hu.msgchan = make(chan *nsq.Message, 1024)
		//fmt.Println("Consumer address ", address)
		err = consumer.ConnectToNSQLookupd(address)
		//err = consumer.ConnectToNSQD(address)
		if err != nil {
			return err
		}
		hu.Process()
	}

	return nil
}
