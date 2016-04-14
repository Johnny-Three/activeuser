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

	//环境创建...
	err := EnvBuild()
	CheckError(err)

	//au环境参数传入...
	SetEnv(Pool, Db)

	//测试活动ID
	SetRedis(7806, Pool)

	//策略加载
	if false == strategy.Init(Db) {
		panic("统计策略load错误")
	}

	//对接NSQ
	err = NewConsummer("base_data_upload", "activestat")
	if err != nil {
		panic(err)
	}

	//Consumer开始运行，消费消息
	go func() {
		ConsumerRun("127.0.0.1:4161")
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

				Calcuserscore(uwd.Uid, value, uwd.Walkdays)
			}
		}
	}()

	select {}
}
