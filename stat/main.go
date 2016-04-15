package main

import (
	. "activeuser/austat"
	. "activeuser/envbuild"
	. "activeuser/logs"
	. "activeuser/nsq"
	. "activeuser/redisop"
	"activeuser/strategy"
	"flag"
	"fmt"
	"os"
	"strings"
)

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

	//对接NSQ
	err = NewConsummer("base_data_upload", "activestat")
	if err != nil {
		panic(err)
	}

	//au环境参数传入...
	SetEnv(Pool, Db)

	//Consumer开始运行，消费消息
	go func() {
		ConsumerRun(EnvConf.Consumerip + ":" + EnvConf.Consumerport)
	}()

	//统计
	go func() {

		for {

			uwd := <-Userwalkdata_chan

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
					CalcuserscoreF(uwd.Uid, value, uwd.Walkdays)
				} else {
					Calcuserscore(uwd.Uid, value, uwd.Walkdays)
				}
			}
		}
	}()

	select {}
}
