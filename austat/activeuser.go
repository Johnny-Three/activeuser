package austat

import (
	. "activeuser/activerule"
	. "activeuser/logs"
	"database/sql"
	"fmt"
	"github.com/garyburd/redigo/redis"
	//"strconv"
	//"os"
	"sync"
	"time"
)

var def = 100
var pool *redis.Pool
var db *sql.DB
var Userday_chan chan *Userdaystat_s
var walkuserdata_chan chan *walkuserdata
var wg sync.WaitGroup

type walkuserdata struct {
	ura *Uarg_s
	wds *[]WalkDayData
}

func init() {

	Userday_chan = make(chan *Userdaystat_s, 16)
	walkuserdata_chan = make(chan *walkuserdata, 16)
}

//将个人天对DB的操作与计算分开，这样最大化的利用CPU的计算能力
func ReadUserDayChan(n int, start time.Time) {

	for {

		uds := <-Userday_chan
		n += 1

		go func(uds *Userdaystat_s) {

			err := HandleUserDayDB(uds, db)
			if err != nil {

				Logger.Error("in HandleUserDayDB", err, "uid:", uds.Uid, "gid", uds.Gid)
			}
			//fmt.Printf("write [%d] recorde into wanbu_stat_activeuser_v1_n0\n", Test)

		}(uds)

		//fmt.Printf("receive num count is %d,uid is %d\n", n, uds.Uid)
	}

}

/*
批次读取函数，由于资源的压力，比如redis，goroutine；
期望一次性不要搞太多的goroutine出来，会占用比较多的内存和CPU；
以及对redis瞬间产生大的连接数；这些都会导致程序问题，因此，这里搞一个批次的概念出来；
一次处理一批，其中批次的批次内数量可以定义。
推荐值<1000,目前envbuild文件中Redispool中的最大连接数设置值为1000，
超过此值有危险
*/
func BatchStat(uids []Uarg_s, dbin *sql.DB, poolin *redis.Pool) {

	pool = poolin
	db = dbin

	fmt.Println("total user is: ", len(uids))
	stepth := len(uids) / def
	fmt.Println("stepth is: ", stepth)

	go func() {

		for {

			wud := <-walkuserdata_chan
			//fmt.Println(wud.ura.Actives)
			for _, aid := range wud.ura.Actives {

				go OneUserActiveStat(wud.ura.Uid, &aid, *wud.wds)
			}
		}

	}()

	for i := 0; i < stepth; i++ {

		//休息一毫秒，这很重要，这一批次内的goroutine将去消费Redis连接，如果批次内数量大于
		//redis设定的最大连接数，将出现connection pool exhausted错误
		time.Sleep(1 * time.Millisecond)

		for j := i * def; j < (i+1)*def; j++ {

			wg.Add(1)

			go func(j int) {
				defer wg.Done()
				wds, err := Loaduserwalkdaydata(uids[j].Uid, db, pool)
				if err != nil {

					Logger.Critical(err)

				} else {
					wud := walkuserdata{}
					wud.wds = &wds
					wud.ura = &uids[j]
					walkuserdata_chan <- &wud
					//fmt.Println(wud)
				}

			}(j)

		}
		wg.Wait()

		fmt.Printf("总[%d]个用户,总[%d]批,第[%d]批读取完毕,此批[%d]个用户\n", len(uids), stepth, i, def)

	}

	yu := len(uids) % def

	//模除部分处理
	if yu != 0 {

		for j := stepth * def; j < len(uids); j++ {

			time.Sleep(1 * time.Millisecond)

			wg.Add(1)
			go func(j int) {
				defer wg.Done()
				wds, err := Loaduserwalkdaydata(uids[j].Uid, db, pool)
				if err != nil {

					Logger.Critical(err)

				} else {

					wud := walkuserdata{}
					wud.wds = &wds
					wud.ura = &uids[j]
					walkuserdata_chan <- &wud
				}

			}(j)
		}

		wg.Wait()
		fmt.Printf("总[%d]个用户,总[%d]批,第[%d]批读取完毕,此批[%d]个用户\n", len(uids), stepth, stepth,
			len(uids[stepth*def:]))

	}

}

func OneUserActiveStat(uid int, arg *Arg_s, wdsin []WalkDayData) {

	ars, exists := ActiveRules[arg.Aid]
	if exists == false {

		Logger.Critical("uid ", uid, " aid ", arg.Aid, " 没有找到对应的ActiveRule..,或许因为用户活动对应查找；与活动规则加载没有对应上")
		return
	}

	wdsout, join := Validstatdays(ars, arg, wdsin)

	if wdsout == nil {

		Logger.Error("user: ", uid, " in active: ", arg.Aid, " upload walkdata between ", wdsin[0].WalkDate, " to ",
			wdsin[len(wdsin)-1].WalkDate, " is invalid .. not stat ..")
		return
	}

	var uds *Userdaystat_s
	//做完一个用户一天的统计后，将结果无情的传出去,供团队天处理..
	//所以结果中应该保留uid,aid及团队统计要用的一切数据..

	for _, wd := range wdsout {

		var ttt []float64

		ot1 := BaseStat(&wd, ars)
		st1 := TaskCreditStat(&wd, ars, uid, db)
		st2 := TimezoneStat(&wd, ars)

		ttt = append(append(append(ttt, ot1), st1...), st2...)

		//fmt.Println(ot1, "+", st1, "+", st2)
		n := TotalScoreStat(ttt)
		//fmt.Printf("credit1 is %v\n", ttt)
		x := StepdistanceStat(n, ars)
		//fmt.Printf("stepdistance is %d\n", x)

		pass, _ := ars.PassRule.Calculate(&wd)

		//st1 3段，credit5\6\7
		if st1 == nil {
			st1 = make([]float64, 3)
		}
		//st2 nil 表示空，st2为2朝三和暮四，st3为3早中晚都有，对应的credit 3、4、8字段
		//st2 为1段，说明积分超上限，只存Credit1
		//st2为2段，朝三加暮四
		if st2 == nil {

			st2 = make([]float64, 3)
		}
		if len(st2) == 1 {

			st2 = append(st2, 0, 0)
		}
		if len(st2) == 2 {

			st2 = append(st2, 0)
		}
		//fmt.Printf(" %v\n", st2)
		uds = &Userdaystat_s{

			arg.Aid,
			uid,
			wd.WalkDate,
			wd.Timestamp,
			arg.Gid,
			wd.Daydata,
			x,
			0,
			n,
			ot1,
			st2[0],
			st2[1],
			st1[0],
			st1[1],
			st1[2],
			st2[2],
			pass,
		}

		err := HandleUserDayDB(uds, db)
		if err != nil {

			Logger.Error("in HandleUserDayDB", err, "uid:", uds.Uid, "gid", uds.Gid)
		}

		//Userday_chan <- uds
	}

	//个人总统计（入DB）
	//比较用户加入活动的时间与活动开始的时间，取其中的大值，作为Start;
	//上传的有效天数据slice的最后一个元素的walkdate作为end;
	err := HandleUserTotalDB(join, wdsout[len(wdsout)-1].WalkDate, uds.Uid, arg, ars, db)
	if err != nil {

		Logger.Error("in HandleUserTotalDB", err, "uid:", uid, "gid", arg.Gid)
	}
	/*
		go func(uds *Userdaystat_s, join int64, wdsout []WalkDayData, arg *Arg_s, ars *ActiveRule) {

			err := HandleUserTotalDB(join, wdsout[len(wdsout)-1].WalkDate, uds.Uid, arg, ars, db)
			if err != nil {

				Logger.Error("in HandleUserTotalDB", err, "uid:", uid, "gid", arg.Gid)
			}

		}(uds, join, wdsout, arg, ars)
	*/

}
