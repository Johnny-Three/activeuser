package main

import (
	. "activeuser/activerule"
	. "activeuser/austat"
	. "activeuser/envbuild"
	. "activeuser/logs"
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
	allusers, err0 := Loadallusers(Db)
	CheckError(err0)

	//load rules from live activities ..
	err1 := LoadAcitveRule(Db)
	CheckError(err1)

	runtime.GOMAXPROCS(runtime.NumCPU())

	go ReadUserDayChan(0, time.Now())
	go BatchStat(allusers, Db, Pool)

	for {

		time.Sleep(1 * time.Second)
	}

}
