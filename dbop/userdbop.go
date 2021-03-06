package dbop

import (
	. "activeuser/logs"
	. "activeuser/structure"
	"database/sql"
	"errors"
	"sync/atomic"
)

var Map_user_actives map[int][]Arg_s

var tcount0 uint32
var tcount1 uint32

//从个人天表中拿到一些数据统计出结果，活动开始到这次统计之间
//比较用户加入活动的时间与活动开始的时间，取其中的大值，作为Start;
//算出这次统计的总成绩，相加
//到达终点写wanbu_snapshot_activeuser_v1表
func HandleUserTotalDB(uid int, arg *Arg_s, ars *ActiveRule, tablev, tablen string, start, end int64, db *sql.DB) error {

	var snapend int64

	qs := `SELECT stepdistance,(CASE WHEN stepnumber>=10000 THEN 1 ELSE 0 END),stepnumber,
	credit1,credit2,credit3,credit4,credit5,credit6,credit7,credit8,stepdaypass,timestamp,walkdate
	FROM wanbu_stat_activeuser_day` + tablen
	qs += ` WHERE  activeid=?  AND userid=? AND  
	walkdate>=? AND walkdate<=? ORDER BY walkdate`

	rows, err := db.Query(qs, arg.Aid, uid, start, end)

	if err != nil {
		return errors.New("执行SQL问题1：" + err.Error())
	}
	defer rows.Close()

	var tmp Userdaytotalstat_s = Userdaytotalstat_s{}
	var udts Userdaytotalstat_s = Userdaytotalstat_s{}
	var snap Userdaytotalstat_s
	var ifarrive = false

	for rows.Next() {

		err := rows.Scan(&udts.Stepdistance, &udts.Stepdaywanbu, &udts.Stepnumber,
			&udts.Credit1, &udts.Credit2, &udts.Credit3, &udts.Credit4, &udts.Credit5, &udts.Credit6,
			&udts.Credit7, &udts.Credit8, &udts.Stepdaypass, &udts.Timestamp, &udts.Walkdate)

		if err != nil {
			return errors.New("执行SQL问题2：" + err.Error())
		}

		//从加入活动算到上传数据最后一天日期...
		tmp.Stepdistance += udts.Stepdistance
		tmp.Stepdaywanbu += udts.Stepdaywanbu
		tmp.Stepnumber += udts.Stepnumber
		//特别需要注意credit1如果等于0，因为如果没有贡献值，timestamp和walkdate不需要更新
		if udts.Credit1 == 0 {

			tmp.Credit1 += udts.Credit1
		} else {

			tmp.Credit1 += udts.Credit1
			tmp.Timestamp = udts.Timestamp
			tmp.Walkdate = udts.Walkdate
		}
		tmp.Credit2 += udts.Credit2
		tmp.Credit3 += udts.Credit3
		tmp.Credit4 += udts.Credit4
		tmp.Credit5 += udts.Credit5
		tmp.Credit6 += udts.Credit6
		tmp.Credit7 += udts.Credit7
		tmp.Credit8 += udts.Credit8
		tmp.Stepdaypass += udts.Stepdaypass

		//准备素材，稍后看是否需要插入或更新snap_shot表..
		if tmp.Stepdistance >= ars.Enddistance && ifarrive == false {

			snap = Userdaytotalstat_s{}
			snap.Stepdistance = ars.Enddistance
			snap.Stepdaywanbu = tmp.Stepdaywanbu
			snap.Stepnumber = tmp.Stepnumber
			snap.Credit1 = tmp.Credit1
			snap.Credit2 = tmp.Credit2
			snap.Credit3 = tmp.Credit3
			snap.Credit4 = tmp.Credit4
			snap.Credit5 = tmp.Credit5
			snap.Credit6 = tmp.Credit6
			snap.Credit7 = tmp.Credit7
			snap.Credit8 = tmp.Credit8
			snap.Stepdaypass = tmp.Stepdaypass
			snap.Timestamp = tmp.Timestamp
			snap.Walkdate = tmp.Walkdate
			//snapend计算DATEDIFF(FROM_UNIXTIME(?),FROM_UNIXTIME(?))+1，stepdaysp(阶段步行天数)
			snapend = snap.Walkdate

			ifarrive = true
		}
	}

	//未到达终点，将tmp中的数据更新至用户总统计表中。。(到不到终点都要更新)
	tablename := "wanbu_stat_activeuser" + tablev
	//stepdaysp用DATEDIFF算出来，从用户加入活动时间到当前时间（）
	is := "insert into " + tablename + ` (activeid,userid,timestamp,stepdaysp,stepdaywanbup,stepnumberp,
				stepdistancep,credit1p,credit2p,credit3p,credit4p,updatetime,arrivetime,stepdaypassp,credit5p,credit6p,
				walkdate,credit7p,credit8p) values
		        (?,?,?,DATEDIFF(FROM_UNIXTIME(?),FROM_UNIXTIME(?))+1,
		        ?,?,?,?,?,?,?,UNIX_TIMESTAMP(),0,?,?,?,?,?,?)
		        ON DUPLICATE KEY UPDATE
		        timestamp = values(timestamp),stepdaysp = values(stepdaysp),stepdaywanbup = values(stepdaywanbup),
		        stepnumberp = values(stepnumberp),stepdistancep=values(stepdistancep),credit1p=VALUES(credit1p),
		        credit2p=VALUES(credit2p),credit3p=VALUES(credit3p),credit4p=VALUES(credit4p),credit5p=VALUES(credit5p),
		        credit6p=VALUES(credit6p),credit7p=VALUES(credit7p),credit8p=VALUES(credit8p),updatetime=UNIX_TIMESTAMP(),
		        walkdate=VALUES(walkdate),stepdaypassp=VALUES(stepdaypassp)`

	_, err1 := db.Exec(is, arg.Aid, uid, tmp.Timestamp, end, start, tmp.Stepdaywanbu, tmp.Stepnumber,
		tmp.Stepdistance, tmp.Credit1, tmp.Credit2, tmp.Credit3, tmp.Credit4, tmp.Stepdaypass, tmp.Credit5,
		tmp.Credit6, tmp.Walkdate, tmp.Credit7, tmp.Credit8)

	if err1 != nil {

		return errors.New("执行SQL问题3：" + err1.Error())
	}

	//到达终点，需要查看到达日期是否提前，如果提前不操作snapshot表，保持arrivetime不变
	//如果arrivetime向后延迟，则更新snapshot表
	if ifarrive == true {

		//查看wanbu_snapshot_activeuser_X表中的arrivetime，如果不存在查出来为0（有用）
		qs := "select IFNULL(sum(arrivetime),0) from wanbu_snapshot_activeuser" + tablev
		qs += " where activeid=?  AND userid=?"

		rows, err := db.Query(qs, arg.Aid, uid)
		if err != nil {
			return errors.New("执行SQL问题4：" + err.Error())
		}
		defer rows.Close()
		var art int64
		for rows.Next() {

			err := rows.Scan(&art)

			if err != nil {
				return errors.New("执行SQL问题5：" + err.Error())

			}
		}

		//如果snapshot表中的arrivetime和当前加和的天数据最后一天有相同日期的情况下，需要更新snapshot表，意思是
		//引起成绩变化（到终点），当天的成绩仍然需要继续统计
		if snap.Walkdate == art {

			tablename = "wanbu_snapshot_activeuser" + tablev
			is := "insert into " + tablename + `(activeid,userid,timestamp,stepdaysp,stepdaywanbup,
				stepnumberp,stepdistancep,credit1p,credit2p,credit3p,credit4p,updatetime,arrivetime,stepdaypassp,
				credit5p,credit6p,credit7p,credit8p) values 
        (?,?,?,DATEDIFF(FROM_UNIXTIME(?),FROM_UNIXTIME(?))+1,
        ?,?,?,?,?,?,?,UNIX_TIMESTAMP(),?,?,?,?,?,?)
        ON DUPLICATE KEY UPDATE 
        stepdaysp = values(stepdaysp),stepdaywanbup = values(stepdaywanbup),
        stepnumberp = values(stepnumberp),stepdistancep=values(stepdistancep),credit1p=VALUES(credit1p),
        credit2p=VALUES(credit2p),credit3p=VALUES(credit3p),credit4p=VALUES(credit4p),credit5p=VALUES(credit5p),
        credit6p=VALUES(credit6p),credit7p=VALUES(credit7p),credit8p=VALUES(credit8p),updatetime=UNIX_TIMESTAMP(),
        stepdaypassp=VALUES(stepdaypassp)`

			_, err0 := db.Exec(is, arg.Aid, uid, snap.Timestamp, snapend, start, snap.Stepdaywanbu, snap.Stepnumber,
				snap.Stepdistance, snap.Credit1, snap.Credit2, snap.Credit3, snap.Credit4, snap.Walkdate,
				snap.Stepdaypass,
				snap.Credit5, snap.Credit6, snap.Credit7, snap.Credit8)

			if err0 != nil {
				//fmt.Println("insert into wanbu_snapshot_activeuser " + err0.Error())
				return errors.New("insert into wanbu_snapshot_activeuser " + err0.Error())
			}

		}

		//如果tmp.Walkdate>arrivetime,更新snapshot
		if snap.Walkdate > art {

			/*
				CREATE TABLE `wanbu_snapshot_activeuser_v1` (
				  `activeid` mediumint(8) NOT NULL,
				  `userid` mediumint(8) NOT NULL,
				  `timestamp` int(10) unsigned NOT NULL,
				  `stepdaysp` mediumint(6) NOT NULL DEFAULT '0',
				  `stepdaywanbup` mediumint(6) NOT NULL DEFAULT '0',
				  `stepnumberp` bigint(10) NOT NULL DEFAULT '0',
				  `stepdistancep` bigint(12) NOT NULL DEFAULT '0',
				  `steptimep` bigint(10) NOT NULL DEFAULT '0',
				  `credit1p` double(10,2) NOT NULL DEFAULT '0.00',
				  `credit2p` double(10,2) NOT NULL DEFAULT '0.00',
				  `credit3p` double(10,2) NOT NULL DEFAULT '0.00',
				  `credit4p` double(10,2) NOT NULL DEFAULT '0.00',
				  `updatetime` int(10) unsigned NOT NULL DEFAULT '0',
				  `arrivetime` int(10) unsigned NOT NULL DEFAULT '0',
				  `stepdaypassp` mediumint(6) NOT NULL DEFAULT '0',
				  `credit5p` double(10,2) NOT NULL DEFAULT '0.00',
				  `credit6p` double(10,2) NOT NULL DEFAULT '0.00',
				  `credit7p` double(10,2) NOT NULL DEFAULT '0.00',
				  `credit8p` double(10,2) NOT NULL DEFAULT '0.00',
			*/

			tablename = "wanbu_snapshot_activeuser" + tablev
			is := "insert into " + tablename + `(activeid,userid,timestamp,stepdaysp,stepdaywanbup,
				stepnumberp,stepdistancep,credit1p,credit2p,credit3p,credit4p,updatetime,arrivetime,stepdaypassp,
				credit5p,credit6p,credit7p,credit8p) values 
        (?,?,?,DATEDIFF(FROM_UNIXTIME(?),FROM_UNIXTIME(?))+1,
        ?,?,?,?,?,?,?,UNIX_TIMESTAMP(),?,?,?,?,?,?)
        ON DUPLICATE KEY UPDATE 
        timestamp = values(timestamp),stepdaysp = values(stepdaysp),stepdaywanbup = values(stepdaywanbup),
        stepnumberp = values(stepnumberp),stepdistancep=values(stepdistancep),credit1p=VALUES(credit1p),
        credit2p=VALUES(credit2p),credit3p=VALUES(credit3p),credit4p=VALUES(credit4p),credit5p=VALUES(credit5p),
        credit6p=VALUES(credit6p),credit7p=VALUES(credit7p),credit8p=VALUES(credit8p),updatetime=UNIX_TIMESTAMP(),
        stepdaypassp=VALUES(stepdaypassp),arrivetime=?`

			_, err0 := db.Exec(is, arg.Aid, uid, snap.Timestamp, snapend, start, snap.Stepdaywanbu, snap.Stepnumber,
				snap.Stepdistance, snap.Credit1, snap.Credit2, snap.Credit3, snap.Credit4, snap.Walkdate,
				snap.Stepdaypass,
				snap.Credit5, snap.Credit6, snap.Credit7, snap.Credit8, snap.Walkdate)

			if err0 != nil {
				//fmt.Println("insert into wanbu_snapshot_activeuser " + err0.Error())
				return errors.New("insert into wanbu_snapshot_activeuser " + err0.Error())
			}

			us := "update " + "wanbu_stat_activeuser" + tablev
			us += " set arrivetime=? where activeid=? and userid=?"
			_, err1 := db.Exec(us, snap.Walkdate, arg.Aid, uid)

			if err1 != nil {
				return errors.New("执行SQL问题6：" + err1.Error())
			}
		}
	}

	atomic.AddUint32(&tcount0, 1)
	Logger.Infof("个人总：用户【%d】,活动【%d】,起始时间【%d-%d】统计完毕", uid, arg.Aid, start, end)
	Logger.Debugf("write [%d] record into %s", tcount0, "wanbu_stat_activeuser"+tablev)

	return nil
}

func HandleUserDayDB(slice_uds []Userdaystat_s, ars *ActiveRule, tablen string, db *sql.DB) error {

	sqlStr := "INSERT INTO wanbu_stat_activeuser_day" + tablen +
		"(activeid, userid, walkdate,timestamp, updatetime, groupid,stepnumber, stepdistance, steptime, credit1," +
		"credit2, credit3,  credit4, credit5, credit6,credit7,credit8, stepdaypass) values"

	vals := []interface{}{}

	for _, uds := range slice_uds {
		sqlStr += "(?,?,?,?,UNIX_TIMESTAMP(),?,?,?,?,?,?,?,?,?,?,?,?,?),"
		if ars.Systemflag == 1 {
			vals = append(vals, uds.Aid, uds.Uid, uds.Walkdate, uds.Timestamp, uds.Gid, uds.Stepnumber, uds.Stepdistance,
				uds.Steptime, uds.Credit1, uds.Credit2, uds.Credit3, uds.Credit4, uds.Credit5,
				uds.Credit6, uds.Credit7, uds.Credit8, uds.Stepdaypass)
		} else {
			vals = append(vals, uds.Aid, uds.Uid, uds.Walkdate, uds.Timestamp, uds.Gid, uds.Credit1, uds.Stepdistance,
				uds.Steptime, uds.Credit1, uds.Credit2, uds.Credit3, uds.Credit4, uds.Credit5,
				uds.Credit6, uds.Credit7, uds.Credit8, uds.Stepdaypass)
		}
	}
	//trim the last ,
	sqlStr = sqlStr[0 : len(sqlStr)-1]

	sqlStr += `ON DUPLICATE KEY UPDATE timestamp =  IF(stepdistance <> VALUES(stepdistance), VALUES(timestamp), timestamp),
				        updatetime=VALUES(updatetime),credit1=VALUES(credit1),credit2=VALUES(credit2),credit3=VALUES(credit3),
				         credit4=VALUES(credit4),credit5=VALUES(credit5),credit6=VALUES(credit6),credit7=VALUES(credit7),
				         credit8=VALUES(credit8),stepnumber=VALUES(stepnumber),stepdistance=VALUES(stepdistance),
				         steptime=VALUES(steptime),groupid=VALUES(groupid),stepdaypass=VALUES(stepdaypass),updatetime=Values(updatetime)`

	//format all vals at once
	_, err := db.Exec(sqlStr, vals...)

	if err != nil {
		return err
	}

	atomic.AddUint32(&tcount1, 1)
	Logger.Infof("个人天：用户【%d】,活动【%d】,起始时间【%d-%d】统计完毕", slice_uds[0].Uid, slice_uds[0].Aid, slice_uds[0].Walkdate, slice_uds[len(slice_uds)-1].Walkdate)
	Logger.Debugf("write [%d] record into %s", tcount1, "wanbu_stat_activeuser_day"+tablen)

	return nil

}

//todo..需要加入分值。。
func HandleTaskBonusDB(cin *Task_credit_struct, ars *ActiveRule, bonus float64, sd int64, gid int, tablen string, db *sql.DB) error {

	//根据任务加分的type，决定插入c5 or c6
	//需要计算总分数

	us := Userdaystat_s{}
	exist := false
	var updatetime int64
	var stepdaypass int

	//查找当前是否有记录,有的话加上奖励积分
	qs := "SELECT * from  wanbu_stat_activeuser_day" + tablen +
		" where userid=? and activeid = ? and walkdate=?"

	rows, err := db.Query(qs, cin.Userid, cin.Activeid, cin.Date)

	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {

		err := rows.Scan(&us.Aid, &us.Uid, &us.Walkdate, &updatetime, &us.Timestamp, &us.Credit1, &us.Credit2,
			&us.Credit3, &us.Credit4, &us.Stepnumber, &us.Stepdistance, &us.Steptime, &us.Gid, &us.Stepdaypass,
			&us.Credit5, &us.Credit6, &us.Credit7, &us.Credit8)

		if err != nil {
			return err
		}

		exist = true
	}

	//存在记录，则更新某些字段
	if true == exist {

		//类型为任务加分..
		if cin.Type == 0 {

			sqlStr := "update wanbu_stat_activeuser_day" + tablen

			//步数制活动，stepnumber需要与credit1（总积分）保持一致
			if ars.Systemflag == 0 {

				//步数制，加分需要考虑可能stepdaypass字段的变化
				if int(us.Credit1+bonus) >= ars.PassRule.Objectsteps && ars.PassRule.Objectsteps != 0 {
					stepdaypass = 1
				}

				sqlStr += " set timestamp= (case when timestamp=0 then ? else timestamp end),   " +
					"updatetime=UNIX_TIMESTAMP(),stepdistance=?,stepnumber=?,credit1=?,credit5=?,stepdaypass=?  where userid=? and activeid = ? and walkdate=?"

				_, err := db.Exec(sqlStr, cin.Date, us.Stepdistance+sd, us.Credit1+bonus, us.Credit1+bonus, us.Credit5+bonus, stepdaypass, cin.Userid, cin.Activeid, cin.Date)

				if err != nil {
					return err
				}
			} else if ars.Systemflag == 1 {

				sqlStr += " set timestamp= (case when timestamp=0 then ? else timestamp end),   " +
					"updatetime=UNIX_TIMESTAMP(),stepdistance=?,credit1=?,credit5=?  where userid=? and activeid = ? and walkdate=?"

				_, err := db.Exec(sqlStr, cin.Date, us.Stepdistance+sd, us.Credit1+bonus, us.Credit5+bonus, cin.Userid, cin.Activeid, cin.Date)

				if err != nil {
					return err
				}
			}

		} else if cin.Type == 1 { //类型为手动加分

			sqlStr := "update wanbu_stat_activeuser_day" + tablen

			//步数制活动，stepnumber需要与credit1（总积分）保持一致
			if ars.Systemflag == 0 {

				//步数制，加分需要考虑可能stepdaypass字段的变化
				if int(us.Credit1+bonus) >= ars.PassRule.Objectsteps && ars.PassRule.Objectsteps != 0 {
					stepdaypass = 1
				}

				sqlStr += " set timestamp= (case when timestamp=0 then ? else timestamp end),   " +
					"updatetime=UNIX_TIMESTAMP(),stepdistance=?,stepnumber=?,credit1=?,credit6=?,stepdaypass=?  where userid=? and activeid = ? and walkdate=?"

				_, err := db.Exec(sqlStr, cin.Date, us.Stepdistance+sd, us.Credit1+bonus, us.Credit1+bonus, us.Credit6+bonus, stepdaypass, cin.Userid, cin.Activeid, cin.Date)

				if err != nil {
					return err
				}

			} else if ars.Systemflag == 1 {

				sqlStr += " set timestamp= (case when timestamp=0 then ? else timestamp end),  " +
					"updatetime=UNIX_TIMESTAMP(),stepdistance=?,credit1=?,credit6=?  where userid=? and activeid = ? and walkdate=?"

				_, err := db.Exec(sqlStr, cin.Date, us.Stepdistance+sd, us.Credit1+bonus, us.Credit6+bonus, cin.Userid, cin.Activeid, cin.Date)

				if err != nil {
					return err
				}
			}
		}
	}

	//不存在记录，则插入一条记录，此处注意stepnumber需要和credit1一致（步数制情况下）
	if false == exist {

		//类型为任务加分..
		if cin.Type == 0 {

			sqlStr := "INSERT INTO wanbu_stat_activeuser_day" + tablen +
				"(activeid, userid, walkdate,timestamp, updatetime, groupid,stepnumber, stepdistance, steptime, credit1," +
				"credit2, credit3,  credit4, credit5, credit6,credit7,credit8, stepdaypass) values"

			if ars.Systemflag == 0 {

				sqlStr += `(?,?,?,?,UNIX_TIMESTAMP(),?,?,?,0,?,0,0,0,?,0,0,0,?)`
				//步数制，加分需要考虑可能stepdaypass字段的变化
				if int(bonus) >= ars.PassRule.Objectsteps && ars.PassRule.Objectsteps != 0 {
					stepdaypass = 1
				}
				_, err := db.Exec(sqlStr, cin.Activeid, cin.Userid, cin.Date, cin.Date, gid, bonus, sd, bonus, bonus, stepdaypass)
				if err != nil {
					return err
				}

			} else if ars.Systemflag == 1 {
				sqlStr += `(?,?,?,?,UNIX_TIMESTAMP(),?,0,?,0,?,0,0,0,?,0,0,0,0)`
				_, err := db.Exec(sqlStr, cin.Activeid, cin.Userid, cin.Date, cin.Date, gid, sd, bonus, bonus)
				if err != nil {
					return err
				}
			}

		} else if cin.Type == 1 { //类型为手动加分

			sqlStr := "INSERT INTO wanbu_stat_activeuser_day" + tablen +
				"(activeid, userid, walkdate,timestamp, updatetime, groupid,stepnumber, stepdistance, steptime, credit1," +
				"credit2, credit3,  credit4, credit5, credit6,credit7,credit8, stepdaypass) values"

			if ars.Systemflag == 0 {

				//步数制，加分需要考虑可能stepdaypass字段的变化
				if int(bonus) >= ars.PassRule.Objectsteps && ars.PassRule.Objectsteps != 0 {
					stepdaypass = 1
				}

				sqlStr += `(?,?,?,?,UNIX_TIMESTAMP(),?,?,?,0,?,0,0,0,0,?,0,0,?)`
				_, err := db.Exec(sqlStr, cin.Activeid, cin.Userid, cin.Date, cin.Date, gid, bonus, sd, bonus, bonus, stepdaypass)
				if err != nil {
					return err
				}

			} else if ars.Systemflag == 1 {
				sqlStr += `(?,?,?,?,UNIX_TIMESTAMP(),?,0,?,0,?,0,0,0,0,?,0,0,0)`
				_, err := db.Exec(sqlStr, cin.Activeid, cin.Userid, cin.Date, cin.Date, gid, sd, bonus, bonus)
				if err != nil {
					return err
				}
			}
		}

	}
	var addtype string
	if cin.Type == 0 {
		addtype = "任务"
	} else if cin.Type == 1 {
		addtype = "手动"
	}

	Logger.Infof("任务加分：用户【%d】,活动【%d】,加分【%.3f】,类型【%s】,时间【%d】统计完毕", cin.Userid, cin.Activeid, bonus, addtype, cin.Date)

	return nil
}
