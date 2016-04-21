package usensq

import (
	"encoding/json"
	"fmt"
)

/*
	{
	 "userdata" :
		[
			{
				"userid" :1,
				"activeid":1 ,
				"groupid":1 ,
				"minwalkdate" :1 ,
				"maxwalkdate"  :1 ,
			},
			{
				"userid" :1,
				"activeid":1 ,
				"groupid":1 ,
				"minwalkdate" :1 ,
				"maxwalkdate"  :1 ,
			}
		]
	}
*/

var Write_nsq_chan chan string

type Write_nsq_struct struct {
	Userdata []Write_node_struct `json:"userdata"`
}

type Write_node_struct struct {
	Userid      int   `json:"userid"`
	Activeid    int   `json:"activeid"`
	Groupid     int   `json:"groupid"`
	Minwalkdate int64 `json:"minwalkdate"`
	Maxwalkdate int64 `json:"maxwalkdate"`
}

func Encode(msg Write_nsq_struct) error {

	value, err := json.Marshal(msg)

	if err != nil {
		return err
	}

	fmt.Println(string(value))

	Write_nsq_chan <- string(value)

	return nil
}

func init() {

	Write_nsq_chan = make(chan string, 1024)

}
