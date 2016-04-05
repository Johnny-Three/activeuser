package dbop

import (
	"database/sql"
	//"errors"
	"fmt"
	//"github.com/garyburd/redigo/redis"
	//. "logs"
	//"os"
	//"strconv"
	//"strings"
	. "activeuser/structure"
	"sync/atomic"
	"time"
)

var Map_user_actives map[int][]Arg_s

var tcount0 uint32
var tcount1 uint32
var snapend int64

//从个人天表中拿到一些数据统计出结果，活动开始到这次统计之间
//算出这次统计的总成绩，相加
//到达终点写wanbu_snapshot_activeuser_v1表
func HandleUserTotalDB(start int64, end int64, uid int, arg *Arg_s, ars *ActiveRule, db *sql.DB) error {

	qs := `SELECT stepdistance,(CASE WHEN stepnumber>=10000 THEN 1 ELSE 0 END),stepnumber,
	credit1,credit2,credit3,credit4,credit5,credit6,credit7,credit8,stepdaypass,timestamp,walkdate
	FROM wanbu_stat_activeuser_day_v1_n0 WHERE  activeid=?  AND userid=? AND  
	walkdate>=? AND walkdate<=? ORDER BY walkdate`

	rows, err := db.Query(qs, arg.Aid, uid, start, end)

	if err != nil {
		return err

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
			return err
		}

		//从加入活动算到上传数据最后一天日期...
		tmp.Stepdistance += udts.Stepdistance
		tmp.Stepdaywanbu += udts.Stepdaywanbu
		tmp.Stepnumber += udts.Stepnumber
		tmp.Credit1 += udts.Credit1
		tmp.Credit2 += udts.Credit2
		tmp.Credit3 += udts.Credit3
		tmp.Credit4 += udts.Credit4
		tmp.Credit5 += udts.Credit5
		tmp.Credit6 += udts.Credit6
		tmp.Credit7 += udts.Credit7
		tmp.Credit8 += udts.Credit8
		tmp.Stepdaypass += udts.Stepdaypass
		tmp.Timestamp = udts.Timestamp
		tmp.Walkdate = udts.Walkdate

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

			if ars.Endtime < snap.Walkdate {
				snapend = ars.Endtime
			} else {
				snapend = snap.Walkdate
			}

			ifarrive = true

		}

	}

	today, _ := time.ParseInLocation("20060102", time.Now().Format("20060102"), time.Local)
	if ars.Endtime < today.Unix() {
		end = ars.Endtime
	} else {
		end = today.Unix()
	}

	//未到达终点，将tmp中的数据更新至用户总统计表中。。(到不到终点都要更新)
	is := `insert into wanbu_stat_activeuser_v1 (activeid,userid,timestamp,stepdaysp,stepdaywanbup,stepnumberp,
		stepdistancep,credit1p,credit2p,credit3p,credit4p,updatetime,arrivetime,stepdaypassp,credit5p,credit6p,
		walkdate,credit7p,credit8p) values 
        (?,?,?,DATEDIFF(FROM_UNIXTIME(?),FROM_UNIXTIME(?))+1,
        ?,?,?,?,?,?,?,UNIX_TIMESTAMP(),0,?,?,?,?,?,?)
        ON DUPLICATE KEY UPDATE 
        timestamp = values(timestamp),stepdaysp = values(stepdaysp),stepdaywanbup = values(stepdaywanbup),
        stepnumberp = values(stepnumberp),stepdistancep=values(stepdistancep),credit1p=VALUES(credit1p),
        credit2p=VALUES(credit2p),credit3p=VALUES(credit3p),credit4p=VALUES(credit4p),credit5p=VALUES(credit5p),
        credit6p=VALUES(credit6p),credit7p=VALUES(credit7p),credit8p=VALUES(credit8p),updatetime=UNIX_TIMESTAMP(),
        stepdaypassp=VALUES(stepdaypassp)`

	_, err1 := db.Exec(is, arg.Aid, uid, tmp.Timestamp, end, start, tmp.Stepdaywanbu, tmp.Stepnumber,
		tmp.Stepdistance, tmp.Credit1, tmp.Credit2, tmp.Credit3, tmp.Credit4, tmp.Stepdaypass, tmp.Credit5,
		tmp.Credit6, tmp.Walkdate, tmp.Credit7, tmp.Credit8)

	if err1 != nil {
		return err1
	}

	//到达终点，需要查看到达日期是否提前，如果提前不操作snapshot表，保持arrivetime不变
	//如果arrivetime向后延迟，则更新snapshot表
	if ifarrive == true {

		//查看wanbu_snapshot_activeuser_v1表中的arrivetime，如果不存在查出来为0（有用）
		qs := `select IFNULL(sum(arrivetime),0) from wanbu_snapshot_activeuser_v1   
		where activeid=?  AND userid=?`

		rows, err := db.Query(qs, arg.Aid, uid)
		if err != nil {
			return err
		}
		defer rows.Close()
		var art int64
		for rows.Next() {

			err := rows.Scan(&art)

			if err != nil {
				return err
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

			is := `insert into wanbu_snapshot_activeuser_v1 (activeid,userid,timestamp,stepdaysp,stepdaywanbup,
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
				return err0
			}

			us := `update wanbu_stat_activeuser_v1 set arrivetime=? where activeid=? and userid=?`
			_, err1 := db.Exec(us, snap.Walkdate, arg.Aid, uid)

			if err1 != nil {
				return err1
			}

		}

	}

	atomic.AddUint32(&tcount0, 1)

	fmt.Printf("write [%d] record into wanbu_stat_activeuser_v1\n", tcount0)

	return nil
}

func HandleUserDayDB(slice_uds []Userdaystat_s, db *sql.DB) error {

	sqlStr := `INSERT INTO wanbu_stat_activeuser_day_v1_n0(activeid, userid, walkdate,timestamp, updatetime, groupid,
				stepnumber, stepdistance, steptime, credit1, credit2, credit3,  credit4, credit5, credit6,
				credit7,credit8, stepdaypass) values `

	vals := []interface{}{}

	for _, uds := range slice_uds {
		sqlStr += "(?,?,?,?,UNIX_TIMESTAMP(),?,?,?,?,?,?,?,?,?,?,?,?,?),"
		vals = append(vals, uds.Aid, uds.Uid, uds.Walkdate, uds.Timestamp, uds.Gid, uds.Stepnumber, uds.Stepdistance,
			uds.Steptime, uds.Credit1, uds.Credit2, uds.Credit3, uds.Credit4, uds.Credit5,
			uds.Credit6, uds.Credit7, uds.Credit8, uds.Stepdaypass)
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
	fmt.Printf("write [%d] record into wanbu_stat_activeuser_day_v1_n0\n", tcount1)

	return nil

}

//加载未关闭的活动中所有的用户
func Checkusers(db *sql.DB) (r_map_u_a *map[int][]Arg_s, err error) {

	//a.activetime < b.closetime 确保活动统计结束前加入活动，否则有问题
	qs := `select a.userid,a.activeid,a.groupid,a.activetime from wanbu_group_user a, wanbu_club_online b 
		where a.activeid = b.activeid AND  b.endtime > UNIX_TIMESTAMP()  
		and b.starttime < UNIX_TIMESTAMP() 
		and a.activetime < b.closetime and b.storeflag <2 order by userid desc`

	rows, err := db.Query(qs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	map_u_a := make(map[int][]Arg_s)
	var actives []Arg_s = []Arg_s{}
	var active Arg_s = Arg_s{}
	var former, next int
	for rows.Next() {

		err := rows.Scan(&next, &active.Aid, &active.Gid, &active.Jointime)
		if err != nil {
			return nil, err
		}

		if former == 0 {
			former = next
		}

		if former == next {

			actives = append(actives, active)
			continue
		}

		user := Uarg_s{}
		user.Uid = former
		user.Actives = actives
		map_u_a[user.Uid] = user.Actives

		former = next
		actives = nil
		actives = append(actives, active)

	}

	if actives != nil {

		user := Uarg_s{}
		user.Uid = former
		user.Actives = actives
		map_u_a[user.Uid] = user.Actives
	}
	return &map_u_a, nil

}

//加载未关闭的活动中所有的用户
func Loadallusers(db *sql.DB) (n []Uarg_s, err error) {

	//a.activetime < b.closetime 确保活动统计结束前加入活动，否则有问题
	qs := `select a.userid,a.activeid,a.groupid,a.activetime from wanbu_group_user a, wanbu_club_online b 
		where a.activeid = b.activeid AND  b.endtime > UNIX_TIMESTAMP()  
		and b.starttime < UNIX_TIMESTAMP() 
		and a.activetime < b.closetime and b.storeflag <2 order by userid desc limit 10000`

	rows, err := db.Query(qs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []Uarg_s = []Uarg_s{}
	var actives []Arg_s = []Arg_s{}
	var active Arg_s = Arg_s{}
	var former, next int
	for rows.Next() {

		err := rows.Scan(&next, &active.Aid, &active.Gid, &active.Jointime)
		if err != nil {
			return nil, err
		}

		if former == 0 {
			former = next
		}

		if former == next {

			actives = append(actives, active)
			continue
		}

		user := Uarg_s{}
		user.Uid = former
		user.Actives = actives
		users = append(users, user)

		former = next
		actives = nil
		actives = append(actives, active)

	}

	if actives != nil {

		user := Uarg_s{}
		user.Uid = former
		user.Actives = actives
		users = append(users, user)
	}
	return users, nil

}
