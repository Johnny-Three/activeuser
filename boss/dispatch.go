package main

import (
	. "activeuser/activerule"
	. "activeuser/logs"
	. "activeuser/socket"
	"fmt"
	"github.com/bitly/go-simplejson"
	"strings"
	//"time"
)

type User_walkdays_struct struct {
	Uid      int
	Walkdays []WalkDayData
}

var Userwalkdata User_walkdays_struct

func processmsg(msg string) error {

	js, err := simplejson.NewJson([]byte(msg))
	if err != nil {
		panic(err.Error())
	}

	var wd WalkDayData
	walkdays := []WalkDayData{}
	Userwalkdata = User_walkdays_struct{}

	userid := js.Get("userid").MustInt()
	wd.Timestamp = js.Get("timestamp").MustInt64()
	arr, _ := js.Get("walkdays").Array()

	for index, _ := range arr {

		walkdate := js.Get("walkdays").GetIndex(index).Get("walkdate").MustInt64()
		wd.WalkDate = walkdate

		var err0 error
		walkhour := js.Get("walkdays").GetIndex(index).Get("walkhour").MustString()
		wd.Hourdata, err0 = Slice_Atoi(strings.Split(walkhour, ","))
		if err0 == nil {

			if len(wd.Hourdata) != 24 {
				Logger.Criticalf("uid %d walkdate %d get wrong hourdata %v format", userid, walkdate, wd.Hourdata)
			}
		}

		wd.Daydata = js.Get("walkdays").GetIndex(index).Get("walktotal").MustInt()
		s_recipe := js.Get("walkdays").GetIndex(index).Get("recipe").MustString()
		i_recipe, err1 := Slice_Atoi(strings.Split(s_recipe, ","))
		if err1 == nil {

			if len(i_recipe) != 4 {
				Logger.Criticalf("uid %d walkdate %d get wrong recipe %v format", userid, walkdate, i_recipe)
			}
		}
		//no problem .. then assign the chufang related value..
		wd.Chufangid = i_recipe[0]
		wd.Chufangfinish = i_recipe[1]
		wd.Chufangtotal = i_recipe[2]

		//用户此次上传的数据消息存储在MAP中..
		walkdays = append(walkdays, wd)

	}

	Userwalkdata.Uid = userid
	Userwalkdata.Walkdays = walkdays
	//向thread safe 的 queue写数据
	//queue.Push(Userwalkdata)

	return nil
}

func dispatch() error {

	//依照现有的连接平均依次发送用户的上传数据。。。
	for {

		val := Msgqueue.Poll()
		if val != nil {

			switch val := val.(type) {
			case string:

				//fmt.Println(val)
				//TODO::挂载在Master上必须有au模块，如果没有au模块则需要发出严厉的告警...
				if len(Slice_netconn) > 0 {

					//依照现有的连接平均依次发送用户的上传数据。。。
					if curconn == len(Slice_netconn) {
						curconn = 0
					}
					words := "fuckyou"
					//发送消息时
					Slice_netconn[curconn].Conn.Write([]byte(words))
					fmt.Println("write msg to ", Slice_netconn[curconn].Seq, " conn")
					//fmt.Println("conn length is ", len(Slice_netconn), "== ", msg)
					curconn += 1

				} else {

					//TODO..光杆司令需要做一些储备工作，避免消息丢失。。。
				}

			}

		}
	}

	return nil

}

func main() {

	go func() {

		dispatch()
	}()

	select {}

}
