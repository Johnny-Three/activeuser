package dbop

import (
	. "activeuser/logs"
	. "activeuser/structure"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"strconv"
	"time"
)

var db *sql.DB
var ActiveRules map[int]*ActiveRule

func checkError(err error, aid int) {
	if err != nil {
		Logger.Critical("Activeid: " + strconv.Itoa(aid) + " ," + err.Error())
	}
}

func LoadAcitveRule(db *sql.DB) error {

	ActiveRules = map[int]*ActiveRule{}
	qs := `select co.activeid,reciperule,credit2distance,baserule, prizeflag, prizerule, 
	    prizecondition,stepwidth,addpersonrule,distanceflag,systemflag,endstattype, 
	    stattimeflag, upstepline, downstepline, upPrizeLine,passrule,storeflag,prestarttime,
	    preendtime,starttime,endtime,ifnull(closetime,0),ifnull(enddistance,0) 
		FROM wanbu_club_online co, wanbu_rule_config rc 
		where co.activeid = rc.activeid AND 
		UNIX_TIMESTAMP(NOW()) < co.closetime and co.parentid= -1 `
	start := time.Now()
	rows, err := db.Query(qs)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {

		var prizerule sql.NullString
		var prizecondition sql.NullInt64
		var addperonrule sql.NullString
		var passrule sql.NullString
		var reciperule sql.NullString

		ar := &ActiveRule{}
		err := rows.Scan(&ar.Activeid, &reciperule, &ar.Credit2distance, &ar.BaseRule.Dbstring, &ar.Prizeflag,
			&prizerule, &prizecondition, &ar.Stepwidth, &addperonrule, &ar.Distanceflag,
			&ar.Systemflag, &ar.Endstattype, &ar.Stattimeflag, &ar.Upstepline, &ar.Downstepline, &ar.UpPrizeLine,
			&passrule, &ar.Storeflag, &ar.Prestarttime, &ar.Preendtime, &ar.Starttime, &ar.Endtime, &ar.Closetime,
			&ar.Enddistance)
		if err != nil {
			return err
		}

		//DB中可空的项，用sql.NullString类型判断一次是否为空..
		//朝三暮四规则
		if prizerule.Valid {

			ar.PrizeRule.Dbstring = prizerule.String
			checkError(ar.PrizeRule.Parse(), ar.Activeid)
		}
		if passrule.Valid {
			ar.PassRule.Dbstring = passrule.String
			checkError(ar.PassRule.Parse(), ar.Activeid)
		}
		if addperonrule.Valid {
			ar.AppendPersonRule.Dbstring = addperonrule.String
		}
		if prizecondition.Valid {
			ar.Prizecondition = int(prizecondition.Int64)
		}
		if reciperule.Valid {
			ar.RecipeRule.Dbstring = reciperule.String
			checkError(ar.RecipeRule.Parse(), ar.Activeid)
		}
		checkError(ar.BaseRule.Parse(), ar.Activeid)
		ActiveRules[ar.Activeid] = ar
	}
	end := time.Now()
	fmt.Printf("ActiveRules Number total is %d\n", len(ActiveRules))
	fmt.Println("LoadAcitveRule query total time:", end.Sub(start).Seconds())
	return nil
}
