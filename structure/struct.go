package structure

import (
	. "activeuser/util"
	"errors"
	"sort"
	"strconv"
	"strings"
)

type Arg_s struct {
	Aid      int
	Gid      int
	Jointime int64
	Quittime int64
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

type Node struct {
	Hour  []int
	Steps int
	Score int
}

type Userdaystat_s struct {
	Aid       int
	Uid       int
	Walkdate  int64
	Timestamp int64
	Gid       int
	//updatetime  int
	Stepnumber   int
	Stepdistance int64
	Steptime     int //无修改，写0
	Credit1      float64
	Credit2      float64
	Credit3      float64
	Credit4      float64
	Credit5      float64
	Credit6      float64
	Credit7      float64
	Credit8      float64
	Stepdaypass  int
}

type WalkDayData struct {
	Daydata       int
	Hourdata      []int
	Chufangid     int
	Chufangfinish int
	Chufangtotal  int
	WalkDate      int64
	Timestamp     int64
}

type AtiveInfo struct {
	Aid       int
	Begintime int64
	Endtime   int64
	ActiveRule
}

type ActiveRule struct {
	AppendPersonRule //加人积分 30000*1；60000*2
	BaseRule         //基础积分规则，格式（10000*5;11000*6;），针对积分算
	PrizeRule        //奖励积分的格式，如（5，6，7，8#3000*1;） 朝三暮四午间
	PassRule         //达标步数设置（endtime1*步数1;..），最多设置3个阶段
	RecipeRule       //处方任务的积分规则，33.3*2;66.7*4;100*10;

	Credit2distance float64 //积分与距离的兑换系数，（积分*系数=距离）
	Prizeflag       int     //奖励积分标志，1，有奖励；2，无奖励     针对朝三暮四，有奖励再计算，计算前看奖励执行前提条件
	Prizecondition  int     //奖励执行的前提条件（如已完成步数10000）针对朝三暮四，如未完成目标设定值，则统计积分为0
	Stepwidth       int     //步幅大小，单位厘米

	Distanceflag int //0,实际路程；1，累计步数*70；2，积分*系数；3，平均步数*70
	Systemflag   int // 0，步数制 1,积分制
	Endstattype  int // 0,个人到达终点后个人不再统计；1，个人到达终点后继续统计
	Stattimeflag int //0,数据统计的截止日期为当前时间的前一天；1，数据统计的截止日期为当前时间
	Upstepline   int //统计的步数上限，超过上限按上限统计  针对步数制
	Downstepline int //统计的步数下限，超过下限按下限统计 ，针对步数制
	UpPrizeLine  int //积分奖励上限，超过上限按上限统计,朝三暮四写credit3里，未超过该咋算咋算

	Storeflag int //0,老版竞赛统计；1，新版竞赛统计；2，处方类活动
	Activeid  int //竞赛ID

	Prestarttime int64
	Preendtime   int64
	Starttime    int64
	Endtime      int64
	Closetime    int64

	Enddistance int64
}

//***********************************AppendPersonRule*******************************************
//此规则存在疑问
type AppendPersonRule struct {
	Dbstring string
}

//***********************************RecipeRule*******************************************
type RecipeRule struct {
	Dbstring string
	Mvalue   map[int]int
}

func (t *RecipeRule) Parse() error {

	if t.Dbstring == "" {

		return nil
	}
	//处方任务的积分规则，33.3*2;66.7*4;100*10;
	//去掉规则的结尾符号;
	t.Dbstring = strings.TrimRight(t.Dbstring, ";")

	t.Mvalue = map[int]int{}
	tmps := strings.Split(t.Dbstring, ";")
	for _, tmp := range tmps {

		x := strings.Split(tmp, "*")
		if len(x) != 2 {

			t.Mvalue = nil
			return errors.New("RecipeRule 格式错误：" + t.Dbstring)
		}

		a, err := strconv.ParseFloat(x[0], 32)
		if err != nil {

			t.Mvalue = nil
			return errors.New("RecipeRule 格式错误：" + t.Dbstring)
		}
		b, err := strconv.Atoi(x[1])
		if err != nil {

			t.Mvalue = nil
			return errors.New("RecipeRule 格式错误：" + t.Dbstring)
		}
		t.Mvalue[int(a)] = b
	}

	return nil
}

func (t *RecipeRule) Calculate(wd *WalkDayData) (n float64, err error) {

	if t.Mvalue == nil {

		return -1, errors.New("RecipeRule nil")

	}
	if wd.Chufangfinish == 0 || wd.Chufangtotal == 0 {
		return -1, errors.New("处方数据错误")
	}
	//map 排序 ， 按key值大小
	var keys []int
	for k := range t.Mvalue {
		keys = append(keys, k)
	}

	sort.Ints(keys)

	//区间内判断
	var end int
	var total int
	var finishrate float32
	for index, key := range keys {

		finishrate = (100 * float32(wd.Chufangfinish) / float32(wd.Chufangtotal))

		if int(finishrate) < key {

			if index == 0 {

				end = -1
			} else {

				end = index - 1
			}
			break
		}
		total += 1
	}
	//超过最大值，按最大值处理
	if total == len(keys) {
		end = len(keys) - 1
	}

	if end == -1 {

		return 0, nil
	}

	return float64(t.Mvalue[keys[end]]), nil

}

//***********************************PassRule*******************************************
//此规则存在疑问
type PassRule struct {
	Dbstring    string
	Objectsteps int
}

func (t *PassRule) Parse() error {
	if t.Dbstring == "" {
		return nil
	}
	var err error
	//去掉规则的结尾符号;
	t.Dbstring = strings.TrimRight(t.Dbstring, ";")
	c := strings.Split(t.Dbstring, "*")
	if len(c) != 2 {
		return errors.New("PassRule 格式错误" + t.Dbstring)
	}
	t.Objectsteps, err = strconv.Atoi(c[1])
	if err != nil {
		return errors.New("PassRule 格式错误" + t.Dbstring)
	}
	return nil
}

func (t *PassRule) Calculate(wd *WalkDayData) (n int, err error) {

	if t.Dbstring == "" {
		return 0, nil
	}
	if wd.Daydata >= t.Objectsteps {
		return 1, nil
	} else {
		return 0, nil
	}
}

//***********************************PrizeRule*******************************************

type PrizeRule struct {
	Dbstring string
	Nodes    []*Node
}

//解析PrizeRule
func (t *PrizeRule) Parse() error {
	//朝三暮四和午间，3段为3个都有，2段为朝三暮四
	//5,6,7,8#3000*1;18,19,20,21,22,23#4000*1;
	if t.Dbstring == "" {
		return nil
	}
	//去掉规则的结尾符号;
	t.Dbstring = strings.TrimRight(t.Dbstring, ";")
	tmps := strings.Split(t.Dbstring, ";")

	if len(tmps) < 1 {

		return errors.New("PrizeRule 格式错误，至少一段值")
	}

	for _, tmp := range tmps {

		a := strings.Split(tmp, "#")
		b := strings.Split(a[0], ",")
		g, err := Slice_Atoi(b)
		if err != nil {

			t.Nodes = nil
			return errors.New("PrizeRule 格式错误,请注意检查")
		}
		c := strings.Split(a[1], "*")
		d, _ := strconv.ParseFloat(c[0], 32)
		e, _ := strconv.ParseFloat(c[1], 32)
		t.Nodes = append(t.Nodes, &Node{g, int(d), int(e)})
	}
	return nil
}

//解析PrizeRule
func (t *PrizeRule) Calculate(wd *WalkDayData) (n []float64, err error) {

	if t.Nodes == nil {
		return nil, errors.New("PrizeRule nil")
	}

	var calcv []float64

	//fmt.Printf("%d len\n", len(t.Nodes))

	for _, node := range t.Nodes {

		var hoursteps int
		//5,6,7,8#3000*1;18,19,20,21,22,23#4000*1;
		for _, v := range node.Hour {
			//Attention !! 超过23点不予以考虑暮四成绩
			if v > 23 {
				break
			}
			hoursteps += wd.Hourdata[v]
		}
		if hoursteps >= node.Steps {
			calcv = append(calcv, float64(node.Score))
		} else {
			calcv = append(calcv, 0)
		}

	}
	return calcv, nil
}

//***********************************BaseRule*******************************************
type BaseRule struct {
	Dbstring string
	Mvalue   map[int]int
}

func (t *BaseRule) Parse() error {

	if t.Dbstring == "" {

		return nil
	}
	//"1*0;5000*2;8000*4;10000*5;11000*6;12000*8;14000*10;"
	//去掉规则的结尾符号;
	t.Dbstring = strings.TrimRight(t.Dbstring, ";")

	t.Mvalue = map[int]int{}
	tmps := strings.Split(t.Dbstring, ";")
	for _, tmp := range tmps {

		x := strings.Split(tmp, "*")
		if len(x) != 2 {

			t.Mvalue = nil
			return errors.New("BaseRule 格式错误：" + t.Dbstring)
		}

		a, err := strconv.Atoi(x[0])
		if err != nil {

			t.Mvalue = nil
			return errors.New("BaseRule 格式错误：" + t.Dbstring)
		}
		b, err := strconv.Atoi(x[1])
		if err != nil {

			t.Mvalue = nil
			return errors.New("BaseRule 格式错误：" + t.Dbstring)
		}
		t.Mvalue[a] = b
	}

	return nil
}

func (t *BaseRule) Calculate(wd *WalkDayData) (n float64, err error) {

	if t.Mvalue == nil {

		return 0, errors.New("BaseRule nil")

	}

	//map 排序 ， 按key值大小
	var keys []int
	for k := range t.Mvalue {
		keys = append(keys, k)
	}

	sort.Ints(keys)

	//区间内判断
	var end int
	var total int
	for index, key := range keys {

		if wd.Daydata < key {

			if index == 0 {

				end = -1
			} else {

				end = index - 1
			}
			break
		}
		total += 1
	}
	//超过最大值，按最大值处理
	if total == len(keys) {
		end = len(keys) - 1
	}

	if end == -1 {

		return 0, nil
	}

	return float64(t.Mvalue[keys[end]]), nil

}
