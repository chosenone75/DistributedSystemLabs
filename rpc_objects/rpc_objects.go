package rpc_objects

import (
	"time"
	"fmt"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"crypto/md5"
	"encoding/hex"
)


type Args struct {
	Id, Passwd string
}

type ReturnValue struct{
	/*
	statuscode: 
	1 for authorized failure
	0 for successful access
	*/
	Statuscode int
	/*
	Time:
	"" for author failure (default value)
	exact time string for successful access
	*/
	Time string
}
type ServerTimer struct{
	conn *sql.DB
}

func (st *ServerTimer) connectToDb() {
     if st.conn == nil{
	db,_ := sql.Open("mysql","root:123456@/dissyslab1")
	st.conn = db
     } 
}
func (st *ServerTimer) insertUser(id,passwd string) bool{
     st.connectToDb()
     stmt,err := st.conn.Prepare("insert user  set id = ?,passwd= ?")
     checkError(err)
     res,err := stmt.Exec(id,sign(passwd))
     checkError(err)
     stmt.Close()
     if res != nil {
        return true
     }
     return false
}
func (st *ServerTimer) queryUser(id string) string{
     st.connectToDb()
     stmt,err := st.conn.Prepare("select passwd from user where id =?")
     checkError(err)
     var psd string
     err = stmt.QueryRow(id).Scan(&psd)
     checkError(err)
     stmt.Close()
     return psd
}
func (st *ServerTimer) Register(arg *Args,result *bool) error{
   *result =  st.insertUser(arg.Id,arg.Passwd)
   return nil
}

func (st *ServerTimer) checkClient(id,passwd string) bool {
   if st.queryUser(id) == sign(passwd){
  	return true
   }
   return false
}
func (st *ServerTimer) EchoTime(args *Args, reply *ReturnValue) error {
	
	fmt.Printf("Receive an Request At %s\n",time.Now().Format(time.UnixDate))
	id := args.Id
	passwd := args.Passwd
         
	if st.checkClient(id,passwd){
	   reply.Statuscode = 0
	   reply.Time = time.Now().Format("2006-01-02 15:04:05")
	}else{
		reply.Statuscode = 1
		reply.Time = ""
	}
	return nil
}
// misc
func sign(passwd string) string {
    md5worker := md5.New()
    md5worker.Write([]byte(passwd))
    result :=md5worker.Sum([]byte(""))
    return hex.EncodeToString(result)
}
func checkError(err error) {
	if err != nil {
	   fmt.Println(err.Error())
	}
}

