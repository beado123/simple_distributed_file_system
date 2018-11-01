package main

import (
	"./daemon"
	"os"
	"bufio"
	"fmt"
	"strings"
)

//TCP: 5678, UDP:3456
func main() {
	if len(os.Args) < 2 {
		fmt.Println("Please type in master id!")
		return
	}
	master_id := os.Args[1]
	d, err := daemon.NewDaemon(master_id)
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
				d.CleanOutSdfs()
				go d.PingToMembers()
        			go d.TimeOutCheck()
        			go d.DaemonListenUDP()
				go d.DaemonListenTCP()

			} else if strings.Contains(cmd, "LIST") {
                                d.PrintMembershipList()
                        } else if strings.Contains(cmd, "SELF") {
				d.PrintId()
			} else if strings.Contains(cmd, "get-versions") {
                                d.SendGetVersionRequest(cmd)
                        } else if strings.Contains(cmd, "put") {
				d.SendPutRequest(cmd)
			} else if strings.Contains(cmd, "get") {
				d.SendGetRequest(cmd)
			} else if strings.Contains(cmd, "delete") {
				d.SendDeleteRequest(cmd)
			} else if strings.Contains(cmd, "ls") {
				d.SendLsRequest(cmd)
			} else if strings.Contains(cmd, "store") {
				d.StoreRequest()
			} else {
				fmt.Println("Input does not match any commads!")	
			}
		}
	}
}

