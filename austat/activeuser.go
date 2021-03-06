package austat

import (
	. "activeuser/dbop"
	. "activeuser/envbuild"
	. "activeuser/logs"
	. "activeuser/redisop"
	"activeuser/strategy"
	. "activeuser/structure"
	"activeuser/usensq"
	"database/sql"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"sync"
)

var pool *redis.Pool
var db *sql.DB
var walkuserdata_chan chan *walkuserdata
var wg sync.WaitGroup

type walkuserdata struct {
	ura *Uarg_s
	wds *[]WalkDayData
}

func init() {

	walkuserdata_chan = make(chan *walkuserdata, 16)
}

func SetEnv(poolin *redis.Pool, dbin *sql.DB) {

	pool = Pool
	db = Db
}

func Calcuserscore(uid int, args []Arg_s, wdsin []WalkDayData) {

	if pool == nil {

		fmt.Println("pool is nil ")
	}

	if db == nil {

		fmt.Println("db is nil ")
	}

	for _, arg := range args {

		go func(arg Arg_s) {

			//每个用户每个活动一个协程..
			OneUserActiveStat(uid, &arg, wdsin)

		}(arg)
	}
}

func CalcuserscoreF(uid int, args []Arg_s, wdsin []WalkDayData) {

	if pool == nil {

		fmt.Println("pool is nil ")
	}

	if db == nil {

		fmt.Println("db is nil ")
	}

	for _, arg := range args {

		//过滤活动，存在配置内的活动予以统计
		for _, filteraid := range EnvConf.FilterAids {

			if arg.Aid == filteraid {

				go func(arg Arg_s) {

					//每个用户每个活动一个协程..
					OneUserActiveStat(uid, &arg, wdsin)

				}(arg)
			}
		}
	}
}

func OneUserActiveStat(uid int, arg *Arg_s, wdsin []WalkDayData) {

	//先找策略表，如果加载策略有问题，直接退。。
	tablev, errv := strategy.GetTableV(arg.Aid)
	if errv != nil {
		Logger.Critical("uid【", uid, "】,", errv)
		return
	}

	tablen, errn := strategy.GetTableN(arg.Aid)
	if errn != nil {
		Logger.Critical("uid【", uid, "】,", errn)
		return
	}

	//找到对应的activerule ..
	ars, err := LoadAcitveRule(arg.Aid, pool, db)

	if err != nil {

		Logger.Critical("uid【", uid, "】，", err)
		return
	}

	wdsout, s, e := Validstatdays(ars, arg, wdsin)

	//fmt.Println("arg is ", arg)
	//fmt.Println("validdate is", wdsout)

	if wdsout == nil {

		Logger.Error("user: ", uid, " in active: ", arg.Aid, " upload walkdata between ", wdsin[0].WalkDate, " to ",
			wdsin[len(wdsin)-1].WalkDate, " is invalid .. not stat ..")
		return
	}

	var slice_uds []Userdaystat_s
	var writensq usensq.Write_nsq_struct
	var writenode usensq.Write_node_struct
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
		uds := Userdaystat_s{

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
		slice_uds = append(slice_uds, uds)
	}

	err = HandleUserDayDB(slice_uds, ars, tablen, db)
	if err != nil {

		Logger.Error("in HandleUserDayDB ", err, "uid: ", uid, "gid ", arg.Gid)
	}

	writenode.Userid = uid
	writenode.Activeid = arg.Aid
	writenode.Groupid = arg.Gid
	writenode.Minwalkdate = wdsout[0].WalkDate
	writenode.Maxwalkdate = wdsout[len(wdsout)-1].WalkDate
	writensq.Userdata = append(writensq.Userdata, writenode)
	//encode json 并且发送至NSQ ..
	usensq.Encode(writensq)

	//个人总统计（入DB）
	err = HandleUserTotalDB(uid, arg, ars, tablev, tablen, s, e, db)
	if err != nil {

		Logger.Error("in HandleUserTotalDB:[ ", err, " ],uid:[", uid, "],gid:[", arg.Gid, "]")
	}

}
