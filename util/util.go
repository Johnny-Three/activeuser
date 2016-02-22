package util

import (
	"fmt"
	"github.com/sirupsen/logrus"
	//"io"
	"os"
)

var Logger *logrus.Logger

func init() {

	SetLogger("../log/chufangguolv.log")
}

func SetLogger(filepath string) {

	Logger = logrus.New()
	Logger.Formatter = new(logrus.JSONFormatter)
	logFile, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
	CheckError(err)
	Logger.Out = logFile
	/*
		writers := []io.Writer{
			logFile,
			os.Stdout,
		}
		Logger.Out = io.MultiWriter(writers...)
	*/

}

func CheckError(err error) {
	if err != nil {
		Logger.Fatal(err)
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
