package nsq

import (
	. "activeuser/logs"
	"github.com/bitly/go-nsq"
	"time"
)

var w *nsq.Producer

func publishMsg(topic, msg string) {

	err = w.Publish(topic, []byte(msg))
	if err != nil {

		Logger.Criticalf("往topic【%s】中写msg【%s】出错，消息将在1S后继续发送", topic, msg)
		//发布消息失败，1S后重发此消息
		time.Sleep(1 * time.Second)
		publishMsg(topic, msg)
	}
}

func GoWriteToNsq(topic string) {

	go func() {

		for {
			msg := <-Write_nsq_chan
			publishMsg(topic, msg)
		}
	}()
}

func NewProducer(address, topic string) {

	config := nsq.NewConfig()
	//心跳定义在5s
	config.HeartbeatInterval = 5000000000
	w, err = nsq.NewProducer(address, config)
	if err != nil {
		panic(err)
	}
}
