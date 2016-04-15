package envbuild

import (
	. "activeuser/logs"
	"activeuser/util"
	"database/sql"
	"flag"
	"github.com/garyburd/redigo/redis"
	_ "github.com/go-sql-driver/mysql"
	config "github.com/msbranco/goconfig"
	"strings"
	"time"
)

var Db *sql.DB
var Pool *redis.Pool
var config_file_path string
var cf *config.ConfigFile
var EnvConf *ConfigRead
var err error

type ConfigRead struct {
	Consumerip   string
	Consumerport string
	Producerip   string
	Producerport string
	Filterstatus string
	FilterAids   []int
}

func GetEnvConf() *ConfigRead {
	return EnvConf
}

func ConfigParse() error {

	if cf == nil {
		cf, _ = config.ReadConfigFile(config_file_path)
	}

	EnvConf = &ConfigRead{}

	EnvConf.Consumerip, err = cf.GetString("CONSUMER", "IP")
	if err != nil {
		Logger.Critical("解析配置文件CONSUMER时出错 ", err)
		return err
	}
	EnvConf.Consumerport, err = cf.GetString("CONSUMER", "PORT")
	if err != nil {
		Logger.Critical("解析配置文件CONSUMER时出错 ", err)
		return err
	}

	EnvConf.Producerip, err = cf.GetString("PRODUCER", "IP")
	if err != nil {
		Logger.Critical("解析配置文件PRODUCER时出错 ", err)
		return err
	}
	EnvConf.Producerport, err = cf.GetString("PRODUCER", "PORT")
	if err != nil {
		Logger.Critical("解析配置文件PRODUCER时出错 ", err)
		return err
	}

	EnvConf.Filterstatus, err = cf.GetString("FILTER", "STATUS")
	if err != nil {
		Logger.Critical("解析配置文件FILTER时出错 ", err)
		return err
	}

	//过滤器打开
	if true == strings.EqualFold(EnvConf.Filterstatus, "on") {

		filterAids, _ := cf.GetString("FILTER", "AIDS")
		//拆出需要过滤的活动ID
		EnvConf.FilterAids, err = util.Slice_Atoi(strings.Split(filterAids, ","))
		if err != nil {
			Logger.Critical("解析配置文件AIDS时出错 ", err)
			return err
		}
	}
	return nil
}

func init() {
	flag.StringVar(&config_file_path, "c", "config file", "Use -c <filepath>")
}

func poolInit(server string) *redis.Pool {
	//redis pool
	return &redis.Pool{
		MaxIdle:     10,
		MaxActive:   200, // max number of connections
		IdleTimeout: 1 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				panic(err.Error())
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

//EnvBuild需要正确的解析文件并且初始化DB和Redis的连接。。
func EnvBuild() error {

	//get conf
	cf, _ = config.ReadConfigFile(config_file_path)
	rdip1, _ := cf.GetString("DBCONN1", "IP")
	rdusr1, _ := cf.GetString("DBCONN1", "USERID")
	rdpwd1, _ := cf.GetString("DBCONN1", "USERPWD")
	rdname1, _ := cf.GetString("DBCONN1", "DBNAME")

	rdip1 = rdusr1 + ":" + rdpwd1 + "@tcp(" + rdip1 + ")/" + rdname1 + "?charset=utf8"

	rhost, _ := cf.GetString("REDIS", "RHOST")
	rport, _ := cf.GetString("REDIS", "RPORT")
	rhost = rhost + ":" + rport
	Pool = poolInit(rhost)

	//open db
	Db, _ = sql.Open("mysql", rdip1)
	//defer db1.Close()
	Db.SetMaxOpenConns(100)
	Db.SetMaxIdleConns(10)
	Db.Ping()

	return nil
}
