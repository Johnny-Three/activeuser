package envbuild

import (
	"database/sql"
	"flag"
	"github.com/garyburd/redigo/redis"
	_ "github.com/go-sql-driver/mysql"
	config "github.com/msbranco/goconfig"
	"time"
)

var Db *sql.DB
var Pool *redis.Pool
var config_file_path string

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
	cf, _ := config.ReadConfigFile(config_file_path)
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
