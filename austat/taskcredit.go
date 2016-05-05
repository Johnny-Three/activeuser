package austat

import (
	. "activeuser/dbop"
	. "activeuser/envbuild"
	. "activeuser/logs"
	. "activeuser/redisop"
	"activeuser/strategy"
	. "activeuser/structure"
	"activeuser/usensq"
	"fmt"
	"time"
)

func Calccreditscore(arg *Arg_s, credit *Task_credit_struct) {

	go CreditStat(arg, credit)
}

func CalccreditscoreF(arg *Arg_s, credit *Task_credit_struct) {

	//过滤活动，存在配置内的活动予以统计
	for _, filteraid := range EnvConf.FilterAids {

		if credit.Activeid == filteraid {

			go CreditStat(arg, credit)
		}
	}
}

func CreditStat(arg *Arg_s, credit *Task_credit_struct) {

	if EnvConf.Pool == nil {

		fmt.Println("pool is nil ")
	}

	if EnvConf.Db == nil {

		fmt.Println("db is nil ")
	}

	db, pool := EnvConf.Db, EnvConf.Pool

	//先找策略表，如果加载策略有问题，直接退。。
	tablev, errv := strategy.GetTableV(arg.Aid)
	if errv != nil {
		Logger.Critical("uid【", credit.Userid, "】,", errv)
		return
	}

	tablen, errn := strategy.GetTableN(arg.Aid)
	if errn != nil {
		Logger.Critical("uid【", credit.Userid, "】,", errn)
		return
	}

	//找到对应的activerule ..
	ars, err := LoadAcitveRule(arg.Aid, pool, db)

	if err != nil {

		Logger.Critical("uid【", credit.Userid, "】，", err)
		return
	}

	//wdsin需要转化一下才能顺利传入...
	var wdsin []WalkDayData = []WalkDayData{}
	var wdsit WalkDayData = WalkDayData{}
	wdsit.WalkDate = credit.Date
	wdsin = append(wdsin, wdsit)

	//需要对加分的这天，判断是否在统计期内
	wdsout, join := Validstatdays(ars, arg, wdsin)

	if wdsout == nil {

		Logger.Error("用户ID: ", credit.Userid, " 竞赛ID: ", arg.Aid, "，任务加分时间：", wdsin[0].WalkDate,
			" ，超出统计期限，不予以加分，请理解")
		return
	}

	var writensq usensq.Write_nsq_struct
	var writenode usensq.Write_node_struct

	//计算得分，包括奖励得分和奖励得分引起的stepdistance增加
	bonus, sd := TaskBonusStat(credit.Bonus, ars)

	//加分操作DB
	err = HandleTaskBonusDB(credit, ars, bonus, sd, arg.Gid, tablen, db)
	if err != nil {

		Logger.Error("in HandleTaskBonusDB ", err, "uid: ", credit.Userid, "gid ", arg.Gid)
	}

	t, _ := time.ParseInLocation("20060102", time.Now().Format("20060102"), time.Local)
	//重新统计一下个人在竞赛中的成绩,endtime传入当前时间。
	err = HandleUserTotalDB(join, t.Unix(), credit.Userid, arg, ars, tablev, tablen, db)
	if err != nil {

		Logger.Error("in HandleUserTotalDB", err, "uid:", credit.Userid, "gid", arg.Gid)
	}

	writenode.Userid = credit.Userid
	writenode.Activeid = arg.Aid
	writenode.Groupid = arg.Gid
	writenode.Minwalkdate = wdsout[0].WalkDate
	writenode.Maxwalkdate = wdsout[len(wdsout)-1].WalkDate
	writensq.Userdata = append(writensq.Userdata, writenode)
	//encode json 并且发送至NSQ ..
	usensq.Encode(writensq)
	//fmt.Println("处理完了，老大放心，干干净净！", json)
}
