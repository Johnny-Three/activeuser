package redisop

import (
	. "activeuser/dbop"
	. "activeuser/logs"
	. "activeuser/structure"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"strconv"
	"strings"
	"time"
)

func checkError(err error, aid int) {
	if err != nil {
		Logger.Critical("Activeid: " + strconv.Itoa(aid) + " ," + err.Error())
	}
}

func SetRedis(aid int, pool *redis.Pool) {

	key := "active:" + strconv.Itoa(aid) + ":info"
	ar := ActiveRuleJson{
		Activeid:         7806,
		RecipeRule:       "0*0;33.3*1;66.7*2;100*3",
		Credit2distance:  2,
		BaseRule:         "1*0;6000*4;7000*5;8000*6;9000*7;10000*8",
		Prizeflag:        1,
		PrizeRule:        "5,6,7,8#3000*2;17,18,19,20,21,22,23#4000*2;",
		Prizecondition:   10000,
		Stepwidth:        0,
		AppendPersonRule: "",
		Distanceflag:     2,
		Systemflag:       1,
		Endstattype:      0,
		Stattimeflag:     1,
		Upstepline:       0,
		Downstepline:     0,
		UpPrizeLine:      2,
		PassRule:         "",
		Storeflag:        2,
		Prestarttime:     0,
		Preendtime:       0,
		Starttime:        1451577600,
		Endtime:          1483200000,
		Closetime:        1483200000,
		Enddistance:      300000000,
	}

	//value := `{"activeid":7806,"reciperule":"0*0;33.3*1;66.7*2;100*3;","credit2distance":2,"baserule":"1*0;6000*4;
	//7000*5;8000*6;9000*7;10000*8","prizeflag":1,"prizerule":"5,6,7,8#3000*2;17,18,19,20,21,22,23#4000*2;",
	//"prizecondition":10000,"stepwidth":0,"addpersonrule":"","distanceflag":2,"systemflag":0,"endstattype":0,
	//"stattimeflag":1,"upstepline":0,"downstepline":0,"upprizeline":2,"passrule":"","storeflag":2,"prestarttime":0,
	//"preendtime":0,"starttime":1451577600,"endtime":1483200000,"closetime":1483200000,"enddistance":300000000}`

	value, _ := json.Marshal(ar)
	conn := pool.Get()
	defer conn.Close()

	// 存入redis
	reply, err := conn.Do("SET", key, value)
	if err != nil {
		fmt.Println("in SetRedis(aid int, pool *redis.Pool) ", err)
	}
	if reply == "OK" {
		fmt.Println("success")
	}
}

func LoadAcitveRule(aid int, pool *redis.Pool, db *sql.DB) (*ActiveRule, error) {

	/*
		//tmp begin  直接给一个假数据，模拟不通过缓存走，直接内存读取，观察在某容量下的性能提升。。
		tar := &ActiveRule{
			Activeid: 7806,
			//RecipeRule:       nil,
			Credit2distance: 2,
			//BaseRule:         nil,
			Prizeflag: 1,
			//PrizeRule:        nil,
			Prizecondition: 10000,
			Stepwidth:      0,
			//AppendPersonRule: nil,
			Distanceflag: 2,
			Systemflag:   0,
			Endstattype:  0,
			Stattimeflag: 1,
			Upstepline:   0,
			Downstepline: 0,
			UpPrizeLine:  2,
			//PassRule:         "",
			Storeflag:    2,
			Prestarttime: 0,
			Preendtime:   0,
			Starttime:    1451577600,
			Endtime:      1483200000,
			Closetime:    1483200000,
			Enddistance:  300000000,
		}

		tar.PrizeRule.Dbstring = "5,6,7,8#3000*2;17,18,19,20,21,22,23#4000*2;"
		checkError(tar.PrizeRule.Parse(), tar.Activeid)

		//tar.PassRule.Dbstring = arj.PassRule
		//checkError(ar.PassRule.Parse(), tar.Activeid)

		tar.RecipeRule.Dbstring = "0*0;33.3*1;66.7*2;100*3"
		checkError(tar.RecipeRule.Parse(), tar.Activeid)

		tar.BaseRule.Dbstring = "1*0;6000*4;7000*5;8000*6;9000*7;10000*8"
		checkError(tar.BaseRule.Parse(), tar.Activeid)

		return tar, nil
		//tmp end
	*/

	//start := time.Now()

	key := "active:" + strconv.Itoa(aid) + ":info"
	// get a client from the pool
	conn := pool.Get()
	defer conn.Close()
	// use the client
	reply, err := redis.Bytes(conn.Do("GET", key))
	if err != nil {
		//redis连接失效？需要验证是不是走这里。。
		err := key + ":" + err.Error()
		fmt.Println(err)
		Logger.Critical(err)
		return LoadAcitveRuleFromDB(aid, db)
	}

	var arj ActiveRuleJson
	// 将json解析ActiveRuleJson类型
	errShal := json.Unmarshal(reply, &arj)
	if errShal != nil {
		//不应该出错误，json格式解析错误
		err := key + ":" + errShal.Error()
		fmt.Println(err, " json格式解析错误")
		Logger.Critical(err, " json格式解析错误")
		return LoadAcitveRuleFromDB(aid, db)
	}

	//json数据导入activerule结构
	ar := &ActiveRule{}

	//导入几个关键数据，结构或者需要判断
	if arj.PrizeRule != "" {

		ar.PrizeRule.Dbstring = arj.PrizeRule
		checkError(ar.PrizeRule.Parse(), arj.Activeid)
	}
	if arj.PassRule != "" {
		ar.PassRule.Dbstring = arj.PassRule
		checkError(ar.PassRule.Parse(), arj.Activeid)
	}
	if arj.AppendPersonRule != "" {
		ar.AppendPersonRule.Dbstring = arj.AppendPersonRule
	}
	if arj.Prizecondition >= 0 {
		ar.Prizecondition = int(arj.Prizecondition)
	}
	if arj.RecipeRule != "" {
		ar.RecipeRule.Dbstring = arj.RecipeRule
		checkError(ar.RecipeRule.Parse(), arj.Activeid)
	}
	if arj.BaseRule != "" {
		ar.BaseRule.Dbstring = arj.BaseRule
		checkError(ar.BaseRule.Parse(), arj.Activeid)
	}

	ar.Activeid = arj.Activeid
	ar.Credit2distance = arj.Credit2distance
	ar.Prizeflag = arj.Prizeflag
	ar.Stepwidth = arj.Stepwidth
	ar.Distanceflag = arj.Distanceflag
	ar.Systemflag = arj.Systemflag
	ar.Endstattype = arj.Endstattype
	ar.Stattimeflag = arj.Stattimeflag
	ar.Upstepline = arj.Upstepline
	ar.Downstepline = arj.Downstepline
	ar.UpPrizeLine = arj.UpPrizeLine
	ar.Storeflag = arj.Storeflag
	ar.Prestarttime = arj.Prestarttime
	if arj.Preendtime > 0 {
		t, _ := time.ParseInLocation("20060102", time.Unix(arj.Preendtime, 0).Format("20060102"), time.Local)
		ar.Preendtime = t.Unix()
	}
	ar.Starttime = arj.Starttime
	if arj.Endtime > 0 {
		t, _ := time.ParseInLocation("20060102", time.Unix(arj.Endtime, 0).Format("20060102"), time.Local)
		ar.Endtime = t.Unix()
	}
	ar.Closetime = arj.Closetime
	ar.Enddistance = arj.Enddistance

	//elapsed := time.Since(start)
	//fmt.Println("LoadAcitveRule query total time:", elapsed)
	return ar, nil
}

//todo..redis获取失败，需要从db中拿到..
func GetUserJoinGroupInfo(uid int, pool *redis.Pool) (r_map_u_a *map[int][]Arg_s, err error) {

	map_u_a := make(map[int][]Arg_s)
	var actives []Arg_s = []Arg_s{}
	var active Arg_s = Arg_s{}

	/*
		//tmp begin
		active.Aid, active.Gid, active.Jointime, active.Quittime = 7806, 311392, 1452528000, 1453305600
		actives = append(actives, active)
		map_u_a[uid] = actives
		return &map_u_a, nil
		//tmp end
	*/

	//start := time.Now() // get current time
	setkey := "user:" + strconv.Itoa(uid) + ":groupinfo"
	// get a client from the pool
	conn := pool.Get()
	defer conn.Close()
	// use the client
	reply, err := redis.Values(conn.Do("smembers", setkey))

	if err != nil {

		err := setkey + ":" + err.Error()
		return nil, errors.New(err)
	}

	var strs []string
	if err = redis.ScanSlice(reply, &strs); err != nil {
		return nil, err
	}
	//加载不到数据，说明没有这个key对应的值，需要过滤掉这个用户上传的数据，不做竞赛统计。
	if strs == nil {

		return nil, nil
	}

	for _, value := range strs {

		tmp := strings.Split(value, ";")
		if len(tmp) != 6 {

			return nil, errors.New(setkey + ":cache 数据格式错误")
		}
		active.Aid, err = strconv.Atoi(tmp[0])
		if err != nil {
			return nil, errors.New(setkey + ":aid 解析数据错误，string to int ")
		}
		active.Gid, err = strconv.Atoi(tmp[1])
		if err != nil {
			return nil, errors.New(setkey + ":gid 解析数据错误，string to int ")
		}
		activetime, err0 := strconv.ParseInt(tmp[2], 10, 64)
		if err0 != nil {
			return nil, errors.New(setkey + ":activetime 解析数据错误，string to int ")
		}
		//根据艳超的建议，对activetime进行特殊的处理，特化为当前日期的0点0分0秒。
		//如果有退组，新加入的组的时间也需要重新格式化一下（这个后期放到维护模块做，不在这里做）
		t, _ := time.ParseInLocation("20060102", time.Unix(activetime, 0).Format("20060102"), time.Local)
		active.Jointime = t.Unix()

		active.Inittime, err = strconv.ParseInt(tmp[3], 10, 64)
		if err != nil {
			return nil, errors.New(setkey + ":inittime 解析数据错误，string to int ")
		}
		//如果有退组，新加入的组的时间也需要重新格式化一下（这个后期放到维护模块做，不在这里做）
		if active.Inittime > 0 {
			t, _ := time.ParseInLocation("20060102", time.Unix(active.Inittime, 0).Format("20060102"), time.Local)
			active.Inittime = t.Unix()

		}

		//fmt.Printf("转换前[%d]，转换后[%d]\n", activetime, active.Jointime)
		active.Quittime, err = strconv.ParseInt(tmp[4], 10, 64)
		if err != nil {
			return nil, errors.New(setkey + ":quitdate 解析数据错误，string to int ")
		}
		//需要对quittime进行处理，如果quittime有值，因为quittime是精确到秒级的,特化为当前日期的0点0分0秒。
		//涉及到调整组之后，这一天数据的归属，根据需求，成绩是属于调整之后的组。
		if active.Quittime > 0 {
			t, _ := time.ParseInLocation("20060102", time.Unix(active.Quittime, 0).Format("20060102"), time.Local)
			active.Quittime = t.Unix()
		}

		actives = append(actives, active)
	}

	map_u_a[uid] = actives
	//elapsed := time.Since(start)
	//fmt.Printf("user [%d] get cache,using the time is %v\n", uid, elapsed)
	return &map_u_a, nil
}

//todo..redis获取失败，需要从db中拿到..
func GetUserJoinOneGroup(uid, aid int, ct *Task_credit_struct, pool *redis.Pool) (r_map_u_a *map[int][]Arg_s, err error) {

	map_u_a := make(map[int][]Arg_s)
	var actives []Arg_s = []Arg_s{}
	var active Arg_s = Arg_s{}

	//start := time.Now() // get current time
	setkey := "user:" + strconv.Itoa(uid) + ":groupinfo"
	// get a client from the pool
	conn := pool.Get()
	defer conn.Close()
	// use the client
	reply, err := redis.Values(conn.Do("smembers", setkey))

	if err != nil {

		err := setkey + ":" + err.Error()
		return nil, errors.New(err)
	}

	var strs []string
	if err = redis.ScanSlice(reply, &strs); err != nil {
		return nil, err
	}
	//加载不到数据，说明没有这个key对应的值，需要过滤掉这个用户上传的数据，不做竞赛统计。
	if strs == nil {

		return nil, nil
	}

	for _, value := range strs {

		tmp := strings.Split(value, ";")
		if len(tmp) != 6 {

			return nil, errors.New(setkey + ":cache 数据格式错误")
		}
		active.Aid, err = strconv.Atoi(tmp[0])
		if err != nil {
			return nil, errors.New(setkey + ":aid 解析数据错误，string to int ")
		}
		//找不到，continue
		if active.Aid != aid {
			continue
		}
		//传了个Credit，找到这天，然后看看这天在哪儿，找到为止
		//step1: 找到inittime..
		active.Inittime, err = strconv.ParseInt(tmp[3], 10, 64)
		if err != nil {
			return nil, errors.New(setkey + ":inittime 解析数据错误，string to int ")
		}
		//如果有退组，新加入的组的时间也需要重新格式化一下（这个后期放到维护模块做，不在这里做）
		if active.Inittime > 0 {
			t, _ := time.ParseInLocation("20060102", time.Unix(active.Inittime, 0).Format("20060102"), time.Local)
			active.Inittime = t.Unix()

		}
		//step2 : 找到quittime..
		active.Quittime, err = strconv.ParseInt(tmp[4], 10, 64)
		if err != nil {
			return nil, errors.New(setkey + ":quitdate 解析数据错误，string to int ")
		}
		//需要对quittime进行处理，如果quittime有值，因为quittime是精确到秒级的,特化为当前日期的0点0分0秒。
		//涉及到调整组之后，这一天数据的归属，根据需求，成绩是属于调整之后的组。
		if active.Quittime > 0 {
			t, _ := time.ParseInLocation("20060102", time.Unix(active.Quittime, 0).Format("20060102"), time.Local)
			active.Quittime = t.Unix()
		}

		//如果要加分的这天在这里，赋值Gid
		if ct.Date >= active.Inittime && ct.Date < active.Quittime {

			active.Gid, err = strconv.Atoi(tmp[1])
			if err != nil {
				return nil, errors.New(setkey + ":gid 解析数据错误，string to int ")
			}

			activetime, err0 := strconv.ParseInt(tmp[2], 10, 64)
			if err0 != nil {
				return nil, errors.New(setkey + ":activetime 解析数据错误，string to int ")
			}
			//对activetime进行特殊的处理，特化为当前日期的0点0分0秒。
			//如果有退组，新加入的组的时间也需要重新格式化一下（这个后期放到维护模块做，不在这里做）
			t, _ := time.ParseInLocation("20060102", time.Unix(activetime, 0).Format("20060102"), time.Local)
			active.Jointime = t.Unix()

			//重新赋值quittime
			active.Quittime = 2147483647

			actives = append(actives, active)

			break

		} else {

			continue
		}
	}

	map_u_a[uid] = actives
	//elapsed := time.Since(start)
	//fmt.Printf("user [%d] get cache,using the time is %v\n", uid, elapsed)
	return &map_u_a, nil
}
