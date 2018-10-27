package main

import (
	"./daemon"
	"os"
	"bufio"
	"fmt"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Please type in master id!")
		return
	}
	port := "3456"
	master_id := os.Args[1]
	d, err := daemon.NewDaemon(port, master_id)
  	if err != nil {
    		return
  	}

	for true {
		buf := bufio.NewReader(os.Stdin)
		input, err := buf.ReadBytes('\n')
		if err != nil {
		    fmt.Println(err)
		} else {
			cmd := string(input)
			if strings.Contains(cmd, "JOIN") {
				buf, err := d.JoinGroup()
        			if err != nil {
                			return
        			}
				d.ResponseLIST(buf)
				go d.PingToMembers()
        			go d.TimeOutCheck()
        			go d.DaemonListen()

			} else if strings.Contains(cmd, "put") {
				go SendPutRequest(cmd)
			} else if strings.Contains(cmd, "get") {
				go SendGetRequest(cmd)
			} else if strings.Contains(cmd, "delete") {
				go SendDeleteRequest(cmd)
			} else if strings.Contains(cmd, "ls") {
				go SendLsRequest(cmd)
			} else if strings.Contains(cmd, "store") {
				go StoreRequest()
			} else if strings.Contains(cmd, "get-versions") {
				go SendGetVersionRequest(cmd)
			} else {
				fmt.Println("Input does not match any commads!")	
			}
		}
	}
}

