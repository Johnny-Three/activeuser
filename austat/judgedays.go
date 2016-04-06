package austat

import (
	. "activeuser/structure"
	"time"
)

//判断时间，找到活动开始、统计结束、活动加入时间和上传日期中需要统计的日期..
//如果返回的wds为空，说明无须统计
//为方便用户总统计，返回用户加入活动的时间
func Validstatdays(ars *ActiveRule, arg *Arg_s, wdsin []WalkDayData) (wdsout []WalkDayData, jointime int64) {

	t, _ := time.ParseInLocation("20060102", time.Now().Format("20060102"), time.Local)

	var begin, end int
	var join int64
	var wadsout []WalkDayData = []WalkDayData{}

	//当前日期在预赛统计时间范围内，计算预赛成绩
	if ars.Prestarttime <= t.Unix() && t.Unix() <= ars.Preendtime {

		//看jointime,jointime发挥一档威力
		if arg.Jointime <= ars.Prestarttime {

			join = ars.Prestarttime

		} else if arg.Jointime > ars.Prestarttime && arg.Jointime <= ars.Preendtime {

			join = arg.Jointime

		} else if arg.Jointime > ars.Preendtime {
			//错误数据
			return nil, -1
		}

		lenth := len(wdsin)

		//上传数据日期非常搞笑的落在了统计日期之外，则不予以统计
		if wdsin[lenth-1].WalkDate < join || wdsin[0].WalkDate > ars.Preendtime {
			return nil, -1
		}

		if wdsin[0].WalkDate >= join {

			begin = 0
		} else {

			//一定能找到
			for p, v := range wdsin {
				if v.WalkDate == join {
					begin = p
				}
			}
		}
		//只统计一天的数据
		if lenth == 1 {

			end = begin
		}

		if lenth > 1 {
			//注意：前提，传入的wdsin中的元素一定是日期增长的，（传入参数保证）
			//最大的传入日期与统计截止日期做比较，
			//quitdate与ars.Preendtime取其中的较小值..
			var comparetime int64
			if arg.Quittime < ars.Preendtime {
				comparetime = arg.Quittime
			} else {
				comparetime = ars.Preendtime
			}

			if wdsin[lenth-1].WalkDate > comparetime {

				//一定能找到
				for p, v := range wdsin {
					if v.WalkDate == comparetime {
						end = p
					}
				}

			} else {
				end = lenth - 1
			}
		}
		//构造需要统计的天数据
		for i := begin; i <= end; i++ {

			wadsout = append(wadsout, wdsin[i])
		}

		return wadsout, join

	}

	//当前日期在正式统计时间范围内，计算正式统计成绩
	if ars.Starttime <= t.Unix() && t.Unix() <= ars.Closetime {

		//看jointime,jointime发挥一档威力
		if arg.Jointime <= ars.Starttime {

			join = ars.Starttime

		} else if arg.Jointime > ars.Starttime && arg.Jointime <= ars.Endtime {

			join = arg.Jointime

		} else if arg.Jointime > ars.Endtime {
			//错误数据
			return nil, -1
		}

		lenth := len(wdsin)

		//上传数据日期非常搞笑的落在了统计日期之外，则不予以统计
		if wdsin[lenth-1].WalkDate < join || wdsin[0].WalkDate > ars.Endtime {
			return nil, -1
		}

		if wdsin[0].WalkDate >= join {

			begin = 0
		} else {

			//一定能找到
			for p, v := range wdsin {
				if v.WalkDate == join {
					begin = p
				}
			}
		}
		//只统计一天的数据
		if lenth == 1 {

			end = begin
		}

		if lenth > 1 {
			//注意：前提，传入的wdsin中的元素一定是日期增长的，（传入参数保证）
			//最大的传入日期与统计截止日期做比较，
			//quitdate与ars.Preendtime取其中的较小值..
			var comparetime int64
			if arg.Quittime < ars.Endtime {
				comparetime = arg.Quittime
			} else {
				comparetime = ars.Endtime
			}

			if wdsin[lenth-1].WalkDate > comparetime {

				//一定能找到
				for p, v := range wdsin {
					if v.WalkDate == comparetime {
						end = p
					}
				}

			} else {
				end = lenth - 1
			}
		}
		//构造需要统计的天数据
		for i := begin; i <= end; i++ {

			wadsout = append(wadsout, wdsin[i])
		}

		return wadsout, join

	}

	//不落在任何区间，无效数据不统计
	return nil, -1
}
