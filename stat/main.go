package main

import (
	. "activeuser/austat"
	. "activeuser/dbop"
	. "activeuser/envbuild"
	. "activeuser/logs"
	. "activeuser/redisop"
	. "activeuser/socket"
	"flag"
	"fmt"
	"os"
	"runtime"
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

	start := time.Now()
	Logger.Info("software begins the time is ", start)

	//load all users need to be calculate ..
	allusers, err0 := Checkusers(Db)
	CheckError(err0)

	//load rules from live activities ..
	err1 := LoadAcitveRule(Db)
	CheckError(err1)

	fmt.Println((*allusers)[454081])
	fmt.Println((*allusers)[454082])
	fmt.Println((*allusers)[454083])
	fmt.Println((*allusers)[454084])
	fmt.Println((*allusers)[454085])
	fmt.Println((*allusers)[454086])
	fmt.Println((*allusers)[454087])
	fmt.Println((*allusers)[454088])
	fmt.Println((*allusers)[454089])
	fmt.Println((*allusers)[454090])

	runtime.GOMAXPROCS(runtime.NumCPU())

	go func() {

		for {

			uwd := <-Userwalkdata_chan

			userinfo, err := GetUserJoinGroupInfo(uwd.Uid, Pool)

			//如果查找用户缓存出现问题...记录问题，继续工作
			if err != nil {
				Logger.Critical(err)
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
