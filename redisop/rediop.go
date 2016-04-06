package redisop

import (
	. "activeuser/structure"
	"errors"
	//"fmt"
	"github.com/garyburd/redigo/redis"
	"strconv"
	"strings"
)

//todo..redis获取失败，需要从db中拿到..
func GetUserJoinGroupInfo(uid int, pool *redis.Pool) (r_map_u_a *map[int][]Arg_s, err error) {

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

	map_u_a := make(map[int][]Arg_s)
	var actives []Arg_s = []Arg_s{}
	var active Arg_s = Arg_s{}

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
		active.Jointime, err = strconv.ParseInt(tmp[2], 10, 64)
		if err != nil {
			return nil, errors.New(setkey + ":activetime 解析数据错误，string to int ")
		}
		active.Quittime, err = strconv.ParseInt(tmp[4], 10, 64)
		if err != nil {
			return nil, errors.New(setkey + ":quitdate 解析数据错误，string to int ")
		}

		actives = append(actives, active)
	}

	map_u_a[uid] = actives
	return &map_u_a, nil
}
