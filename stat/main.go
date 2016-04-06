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
	"time"
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

	go func() {

		for {

			uwd := <-Userwalkdata_chan

			start := time.Now() // get current time
			userinfo, err := GetUserJoinGroupInfo(uwd.Uid, Pool)
			elapsed := time.Since(start)
			fmt.Println("1 user get cache,using the time is ", elapsed)

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

				Calcuserscore(uwd.Uid, value, Db, uwd.Walkdays)
			}
		}
	}()

	select {}
}
