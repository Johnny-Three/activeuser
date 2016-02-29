package activerule

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	//. "logs"
	//"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type Arg_s struct {
	Aid      int
	Gid      int
	Jointime int64
}

type Uarg_s struct {
	Uid     int
	Actives []Arg_s
}

type Userdaytotalstat_s struct {
	Arrivetime   int64
	Stepdaywanbu int
	Userdaystat_s
}

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

func Loaduserwalkdaydata(uid int, db *sql.DB, pool *redis.Pool) (wds []WalkDayData, err error) {

	//假数据，每人两天

	wds1 := []WalkDayData{

		WalkDayData{
			13000,
			[24]int{32, 0, 0, 0, 0, 0, 3000, 544, 0, 696, 492, 673, 1219, 15, 0, 0, 938, 4000, 359, 0, 1148, 6321, 3941, 67},
			3790,
			3,
			3,
			1452873600,
			1455724804,
		},
		WalkDayData{
			13000,
			[24]int{32, 0, 0, 0, 0, 0, 3000, 544, 0, 696, 492, 673, 1219, 15, 0, 0, 938, 4000, 359, 0, 1148, 6321, 3941, 67},
			3790,
			3,
			3,
			1452960000,
			1455724804,
		},

		WalkDayData{
			11616,
			[24]int{0, 0, 0, 0, 0, 0, 0, 0, 1669, 188, 1239, 929, 1577, 494, 1863, 2570, 0, 888, 199, 0, 0, 0, 0, 0},
			25,
			1,
			3,
			1453046400,
			1455811204,
		},
		WalkDayData{
			13000,
			[24]int{32, 0, 0, 0, 0, 0, 3000, 544, 0, 696, 492, 673, 1219, 15, 0, 0, 938, 4000, 359, 0, 1148, 6321, 3941, 67},
			3790,
			3,
			3,
			1453132800,
			1455724804,
		},
		WalkDayData{
			13000,
			[24]int{32, 0, 0, 0, 0, 0, 3000, 544, 0, 696, 492, 673, 1219, 15, 0, 0, 938, 4000, 359, 0, 1148, 6321, 3941, 67},
			3790,
			3,
			3,
			1455897600,
			1455897600,
		},
		WalkDayData{
			13000,
			[24]int{32, 0, 0, 0, 0, 0, 3000, 544, 0, 696, 492, 673, 1219, 15, 0, 0, 938, 4000, 359, 0, 1148, 6321, 3941, 67},
			3790,
			3,
			3,
			1455984000,
			1455984000,
		},
		WalkDayData{
			13000,
			[24]int{32, 0, 0, 0, 0, 0, 3000, 544, 0, 696, 492, 673, 1219, 15, 0, 0, 938, 4000, 359, 0, 1148, 6321, 3941, 67},
			3790,
			3,
			3,
			1456070400,
			1456070400,
		},
	}
	return wds1, nil

	//刷数据，所有用户刷同样的两天数据，今天和昨天的，如果没有数据则返回空值，相当于统计两天的数据
	//不同的是，这里如果从Redis中拿不到这两天的数据，至少，返回最近一天的wds值，这个wds的walkdate为今天unixtimestamp,其余null
	var start, end int64

	today, _ := time.ParseInLocation("20060102", time.Now().AddDate(0, 0, -37).Format("20060102"), time.Local)
	yesterday, _ := time.ParseInLocation("20060102", time.Now().AddDate(0, 0, -38).Format("20060102"), time.Local)

	start = yesterday.Unix()
	end = today.Unix()

	sortedkey := "sortset:uid:" + strconv.Itoa(uid) + ":walkdata"

	conn := pool.Get() // get a client from the pool
	// use the client
	reply, err := redis.Values(conn.Do("ZRANGEBYSCORE", sortedkey, start, end, "withscores"))

	if err != nil {

		err := sortedkey + ":" + err.Error()
		return nil, errors.New(err)
	}

	conn.Close()

	var strs []string
	if err = redis.ScanSlice(reply, &strs); err != nil {
		return nil, err
	}
	//fmt.Println(strs)
	//加载不到元素，todo..需要补数据？

	if strs == nil {

		err := sortedkey + "start: " + strconv.FormatInt(start, 10) + " end: " + strconv.FormatInt(end, 10) + " 数据为空"
		return nil, errors.New(err)
	}

	var daysdata []WalkDayData = []WalkDayData{}
	var daydata WalkDayData = WalkDayData{}

	for index, v := range strs {

		if index%2 != 0 {

			daydata.WalkDate, _ = strconv.ParseInt(v, 10, 64)
			daysdata = append(daysdata, daydata)
			continue
		}

		tmp := strings.Split(v, "#")
		if len(tmp) != 4 {

			return nil, errors.New(sortedkey + "解析格式错误")
		}
		daydata.Daydata, _ = strconv.Atoi(tmp[0])

		hours := strings.Split(tmp[1], ":")
		if len(hours) == 24 {

			for index, value := range hours {
				daydata.Hourdata[index], _ = strconv.Atoi(value)
			}
		}
		chufang := strings.Split(tmp[2], ":")
		if len(chufang) == 3 {
			daydata.Chufangid, _ = strconv.Atoi(chufang[0])
			daydata.Chufangfinish, _ = strconv.Atoi(chufang[1])
			daydata.Chufangtotal, _ = strconv.Atoi(chufang[2])
		}
		daydata.Timestamp, _ = strconv.ParseInt(tmp[3], 10, 64)

	}

	return daysdata, nil

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
