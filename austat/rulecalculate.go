package austat

import (
	. "activeuser/logs"
	. "activeuser/structure"
	"database/sql"
	//fmt
)

//总成绩计算，Credit2+...Credit8
func TotalScoreStat(scores []float64) (n float64) {

	var rv float64
	for _, v := range scores {

		rv += v
	}
	return rv
}

//步行总距离统计,写stepdistance字段
//对于步数制活动，总积分*步幅
//对于积分制活动，积分兑换系数*100000*总积分
func StepdistanceStat(credit1 float64, ar *ActiveRule) (n int64) {

	var rv int64
	if ar.Systemflag == 0 {
		//对0特殊处理一下，如果网站失误写成负数
		if ar.Stepwidth <= 0 {
			ar.Stepwidth = 100
		}
		rv = int64(credit1) * int64(ar.Stepwidth)

	}
	if ar.Systemflag == 1 {

		rv = int64(ar.Credit2distance * 100000 * float64(credit1))
	}
	return rv
}

//加分统计..
func TaskBonusStat(credit float64, ar *ActiveRule) (float64, int64) {

	//步数制活动，需要将C5处理一下，C5从里程变换为步数
	if ar.Systemflag == 0 {
		//对0特殊处理一下，如果网站失误写成负数
		if ar.Stepwidth <= 0 {
			ar.Stepwidth = 100
		}
		credit = credit * 100000 / float64(ar.Stepwidth)
	}

	stepdistance := StepdistanceStat(credit, ar)

	return credit, stepdistance
}

//任务奖励统计，写credit5\credit6\credit7字段。credit5为任务奖励、credit6为手动加分奖励、credit7为处方奖励
//错误情况返回nil，TODO：：修正此方法，这个只计算C7，把统计分开两路，一个数据上传，一个
func TaskCreditStat(wd *WalkDayData, ar *ActiveRule, uid int, db *sql.DB) (n []float64) {

	var c5 float64
	var c6 float64
	var c7 float64
	var rv []float64

	//start := time.Now()

	qs := "SELECT IFNULL(SUM(CASE WHEN taskid <> -1 THEN credit ELSE 0 END),0), IFNULL(SUM(CASE WHEN taskid = -1 " +
		"THEN credit ELSE 0 END),0) from wanbu_member_credit where userid=? and activeid = ? and walkdate=?"

	rows, err := db.Query(qs, uid, ar.Activeid, wd.WalkDate)

	if err != nil {
		Logger.Critical("in TaskCreditStat err happens " + err.Error())
		return nil
	}

	defer rows.Close()
	for rows.Next() {

		err := rows.Scan(&c5, &c6)
		if err != nil {
			Logger.Critical("in TaskCreditStat err happens " + err.Error())
			return nil
		}
		//步数制活动，需要将C5处理一下，C5从里程变换为步数
		if ar.Systemflag == 0 {
			//对0特殊处理一下，如果网站失误写成负数
			if ar.Stepwidth <= 0 {
				ar.Stepwidth = 100
			}
			c5 = c5 * 100000 / float64(ar.Stepwidth)
			c6 = c6 * 100000 / float64(ar.Stepwidth)
		}
	}

	//如果是处方类活动，则统计c7
	if ar.Storeflag == 2 {
		var err error
		c7, err = ar.RecipeRule.Calculate(wd)
		if err != nil {
			c7 = 0
		}
	}

	//end := time.Now()
	//fmt.Println("TaskCreditStat query total time:", end.Sub(start).Seconds())

	rv = append(rv, c5, c6, c7)
	return rv
}

//是否达标统计，写stepdaypass字段
//返回1达标；返回0未达标
func PassdayStat(wd *WalkDayData, ar *ActiveRule) (n int) {

	rv, _ := ar.PassRule.Calculate(wd)
	return rv
}

//基础积分统计，写credit2字段
//积分制活动计算基础积分（baserule）写入，步数制活动写实际步数
func BaseStat(wd *WalkDayData, ar *ActiveRule) (n float64) {

	var rv float64
	//步数制，查看Upstepline，Downstepline,看当天步数是否在上下限内
	//Downstepline,Upstepline初始值为0，需要判断Upstepline为0的情况，不予以处理
	if ar.Systemflag == 0 {

		if ar.Upstepline > 0 {

			if wd.Daydata < ar.Downstepline {

				rv = 0
			}
			if wd.Daydata >= ar.Downstepline && wd.Daydata <= ar.Upstepline {

				rv = float64(wd.Daydata)
			}
			if wd.Daydata > ar.Upstepline {

				rv = float64(ar.Upstepline)
			}

		}

		if ar.Upstepline == 0 {

			if wd.Daydata < ar.Downstepline {
				rv = 0
			} else {
				rv = float64(wd.Daydata)
			}

		}
	}
	//积分制，从baserule中解析出基础积分
	if ar.Systemflag == 1 {

		n, err := ar.BaseRule.Calculate(wd)
		if err != nil {
			rv = 0
		}
		rv = n
	}

	return rv
}

//朝三暮四统计，分时间段，传入参数（天数据，竞赛规则）
//传出参数，slice int，有几段算几段 credit3\credit4\credit8
//传出参数，err为0的时候不做朝三暮四的统计
//返回为空，说明格式有问题，
func TimezoneStat(wd *WalkDayData, ar *ActiveRule) (n []float64) {

	if ar.Prizeflag != 1 {
		return nil
	}

	if wd.Daydata < ar.Prizecondition {
		return nil
	}

	rv, err := ar.PrizeRule.Calculate(wd)
	//计算出错，说明朝三暮四协议格式存储有问题
	if err != nil {

		return nil
	}

	//积分超上限,slice返回值只有一个,只存credit3
	var clt float64
	var srb []float64
	for _, v := range rv {

		clt += v
		//有UpPrizeLine为负数的情况，大于0做数
		if clt > float64(ar.UpPrizeLine) && ar.UpPrizeLine > 0 {
			srb = append(srb, float64(ar.UpPrizeLine))
			break
		}
	}

	if len(srb) > 0 {
		return srb
	}

	return rv
}
