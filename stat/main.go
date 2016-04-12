package main

import (
	. "activeuser/austat"
	. "activeuser/envbuild"
	. "activeuser/logs"
	. "activeuser/redisop"
	. "activeuser/socket"
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

	//environment build
	err := EnvBuild()
	CheckError(err)

	SetEnv(Pool, Db)

	SetRedis(7806, Pool)

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
