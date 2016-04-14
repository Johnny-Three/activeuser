package nsq

import (
	"fmt"
	"github.com/bitly/go-nsq"
	"log"
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
			Decode(string(m.Body))
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

func init() {

	config = nsq.NewConfig()
	//心跳间隔时间
	config.HeartbeatInterval = 3000000000
	//10分钟去发现一次，发现topic为指定的nsqd
	config.LookupdPollInterval = 60000000000

	println("HeartbeatInterval", config.HeartbeatInterval)
	println("MaxAttempts", config.MaxAttempts)
	println("LookupdPollInterval", config.LookupdPollInterval)

	logfile, err := os.OpenFile("../../log/consumer.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("%s\r\n", err.Error())
		os.Exit(-1)
	}
	//defer logfile.Close()
	logger = log.New(logfile, "\r\n", log.Ldate|log.Ltime|log.Llongfile)

}

func NewConsummer(topic, channel string) error {

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

	err = consumer.ConnectToNSQLookupd(address)
	if err != nil {
		return err
	}
	h.Process()
	return nil
}
