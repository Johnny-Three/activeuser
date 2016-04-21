package main

import (
	. "activeuser/austat"
	. "activeuser/envbuild"
	. "activeuser/logs"
	. "activeuser/redisop"
	"activeuser/strategy"
	"activeuser/usensq"
	"flag"
	"fmt"
	"github.com/bitly/go-nsq"
	"os"
	"strings"
)

var consumer *nsq.Consumer

func CheckError(err error) {
	if err != nil {
		Logger.Critical(err)
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func main() {

	flag.Parse()
	defer Logger.Flush()

	//配置文件中的资源初始化（DB,REDIS），环境创建...
	err := EnvBuild()
	CheckError(err)

	//配置文件中其它参数的读写,NSQ consumer IP 及 PORT,NSQ producer IP 及 PROT,活动过滤开关及过滤的活动ID
	err = ConfigParse()
	CheckError(err)

	//测试活动ID
	//SetRedis(7806, Pool)

	//策略加载
	if false == strategy.Init(Db) {
		panic("统计策略load错误")
	}

	//对接NSQ，消费上传消息
	consumer, err = usensq.NewConsummer("base_data_upload", "activestat")
	if err != nil {
		panic(err)
	}

	//Consumer运行，消费消息..
	go func(consumer *nsq.Consumer) {

		err := usensq.ConsumerRun(consumer, "base_data_upload", EnvConf.Consumerip+":"+EnvConf.Consumerport)
		if err != nil {
			panic(err)
		}
	}(consumer)

	//对接NSQ，消费任务消息
	consumer, err = usensq.NewConsummer("task_to_au", "activestat")
	if err != nil {
		panic(err)
	}

	//Consumer运行，消费消息..
	go func(consumer *nsq.Consumer) {

		err := usensq.ConsumerRun(consumer, "task_to_au", EnvConf.Consumerip+":"+EnvConf.Consumerport)
		if err != nil {
			panic(err)
		}
	}(consumer)

	//初始化Producer
	usensq.NewProducer(EnvConf.Producerip+":"+EnvConf.Producerport, "for_gu_stat")

	//au环境参数传入...
	SetEnv(Pool, Db)

	//统计
	go func() {

		for {

			uwd := <-usensq.Userwalkdata_chan

			userinfo, err := GetUserJoinGroupInfo(uwd.Uid, Pool)

			//如果查找用户缓存出现问题...记录问题，继续工作
			if err != nil {
				Logger.Critical(err)
				continue
			}

			if userinfo == nil {

				fmt.Println("uid ", uwd.Uid, " 缓存数据为空")
				continue
			}

			//一定存在，已经从缓存中构造出来数据结构..
			value, exist := (*userinfo)[uwd.Uid]

			if exist == true {

				//如果此配置项打开，需要过滤活动
				if true == strings.EqualFold(EnvConf.Filterstatus, "on") {
					//带过滤项的calc
					CalcuserscoreF(uwd.Uid, value, uwd.Walkdays)
				} else {
					Calcuserscore(uwd.Uid, value, uwd.Walkdays)
				}
			}
		}
	}()

	//生产AG所用消息...
	usensq.GoWriteToNsq("stat_for_au")

	select {}
}
