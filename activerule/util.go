package activerule

import (
	"strconv"
)

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

func Slice_Atoi(strArr []string) ([]int, error) {
	// NOTE:  Read Arr as Slice as you like
	var str string // O
	var i int      // O
	var err error  // O

	iArr := make([]int, 0, len(strArr))
	for _, str = range strArr {
		i, err = strconv.Atoi(str)
		if err != nil {
			return nil, err // O
		}
		iArr = append(iArr, i)
	}
	return iArr, nil
}
