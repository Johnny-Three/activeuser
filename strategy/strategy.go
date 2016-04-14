package strategy

import (
	. "activeuser/logs"
	"database/sql"
	"errors"
	//"fmt"
	"strconv"
)

type item struct {
	Tix int
	Min int
	Max int
}

var st *Strategy

type Strategy struct {
	items []item
	db    *sql.DB
}

//获取stratedy,todo--并且每一个小时更新一次stratedy ..
func Init(dbin *sql.DB) bool {

	//确保只初始化一次
	if st != nil && st.items != nil {

		return true
	}
	st = &Strategy{
		items: nil,
		db:    dbin,
	}

	return st.loadStratedy()

}

func (this *Strategy) loadStratedy() bool {

	qs := `select id,minid,maxid from wanbu_stat_tablestrategy`

	var ts item

	rows, err := this.db.Query(qs)
	if err != nil {
		goto exit
	}
	defer rows.Close()

	for rows.Next() {

		err := rows.Scan(&ts.Tix, &ts.Min, &ts.Max)
		if err != nil {
			goto exit
		}

		st.items = append(st.items, ts)
	}

	return true

exit:
	Logger.Critical("加载策略表出错，err:【", err, "】")
	return false

}

func GetTableV(aid int) (string, error) {

	if st == nil && st.items == nil {

		return "", errors.New("Strategy尚未初始化 ")
	}
	for _, v := range st.items {

		if aid <= v.Max && aid >= v.Min {

			id := strconv.Itoa(v.Tix)
			//wanbu_stat_activeuser_v1
			return "_v" + id, nil
		}
	}
	return "", errors.New("活动ID不在统计策略配置内")

}

func GetTableN(aid int) (string, error) {

	if st == nil && st.items == nil {

		return "", errors.New("Strategy尚未初始化 ")
	}
	for _, v := range st.items {

		if aid <= v.Max && aid >= v.Min {

			id1 := strconv.Itoa(v.Tix)
			id2 := strconv.Itoa(aid % 10)
			//wanbu_stat_activeuser_day_v1_n0
			return "_v" + id1 + "_n" + id2, nil
		}
	}
	return "", errors.New("活动ID不在统计策略配置内")
}
