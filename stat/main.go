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
var version string = "1.0.0PR2"

func CheckError(err error) {
	if err != nil {
		Logger.Critical(err)
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func main() {

	args := os.Args

	if len(args) < 2 {

		fmt.Println("别忘记加 -c config.file 运行哦")
		os.Exit(0)
	}

	if len(args) == 2 && (args[1] == "-v") {

		fmt.Println("看好了兄弟，现在的版本是【", version, "】，可别弄错了")
		os.Exit(0)
	}

	flag.Parse()
	defer Logger.Flush()

	//配置文件中的资源初始化（DB,REDIS），环境创建...
	err := EnvBuild()
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

	//统计口子1====统计从数据上传过来的消息
	go func() {

		for {

			uwd := <-usensq.User_walk_data_chan

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

	//统计口子2====统计从任务系统过来的消息
	go func() {

		for {

			tc := <-usensq.User_task_credit_chan

			//找到这个ACTIVEID，其余的不管
			userinfo, err := GetUserJoinOneGroup(tc.Userid, tc.Activeid, Pool)

			//如果查找用户缓存出现问题...记录问题，继续工作
			if err != nil {
				Logger.Critical(err)
				continue
			}

			if userinfo == nil {

				Logger.Critical("加分请求【", tc, "】", "失败，因为没有正在进行的这个活动，请查证后再加")
				continue
			}

			//一定存在，已经从缓存中构造出来数据结构..
			value, exist := (*userinfo)[tc.Userid]

			if exist == true {

				//如果此配置项打开，需要过滤活动
				if true == strings.EqualFold(EnvConf.Filterstatus, "on") {
					//带过滤项的calc
					CalccreditscoreF(&value[0], &tc)
				} else {
					Calccreditscore(&value[0], &tc)
				}
			}
		}
	}()

	//生产AG所用消息...
	usensq.GoWriteToNsq("stat_for_au")

	select {}
}
