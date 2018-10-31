package main

import (
	"net"
	"os"
	"fmt"
	"os/exec"
	"strings"
	"io/ioutil"
	//"math"
	"strconv"
)

var m map[string][4]string
var version map[string]int
var vm []string
var pointer int
//This function helps printing out errors
func printErr(err error, s string) {
	if err != nil {
		fmt.Println("Error occurs on ", s , "\n" , err.Error())
		os.Exit(1)
	}
}

//This function helps printing out commads that are executing
func printCommand(cmd *exec.Cmd) {
    fmt.Printf("==> Executing: %s\n", strings.Join(cmd.Args, " "))
}

//This function extracts ip address of current VM from file "ip_address" in current directory
func getIPAddrAndLogfile() string{

	data, err := ioutil.ReadFile("ip_address")
	if err != nil {
		panic(err)
	}

	ip := string(data[:len(data)])
	
	//remove \n from end of line
	if strings.HasSuffix(ip, "\n") {
		ip = ip[:(len(ip) - 1)]
	}
	fmt.Println("ip address of current VM:\n", ip)
	return ip
}

func getStorePosition() [4]string{
	arr := [4]string{}
	fmt.Println(pointer,vm)
	if pointer + 1 < 9 {
		arr[0] = vm[pointer+1]
	} else {arr[0] = vm[0]}
	if pointer + 2 < 9 {
		arr[1] = vm[pointer+2]
	} else {arr[1] = vm[(pointer+2-9)]}
	if pointer + 3 < 9 {
		arr[2] = vm[pointer+3]
	} else {arr[2] = vm[pointer+3-9]}
	if pointer + 4 < 9 {
		arr[3] = vm[pointer+4]	
		pointer += 4
	} else {
		arr[3] = vm[pointer+4-9]
		pointer = 9-(pointer+4)
	}
	return arr
}

//This function parses request sent from client and sends the result back to client
//commadn format: query logfile_name
func parseRequest(conn net.Conn) {

	//create a buffer to hold transferred data and read incoming data into buffer
	buf := make([]byte, 1024)
	reqLen, err := conn.Read(buf)
	printErr(err, "reading")

	//convert request command into array
	reqArr := strings.Split(string(buf[:reqLen]), " ")
	
	fmt.Println(reqArr[0], reqArr[1], reqArr[2])

	cmd := reqArr[0]
	out := ""
	if cmd == "put" {
		fileName := reqArr[2]
		_, ok := m[fileName]
		if ok {
			vms := m[fileName]
			out += strconv.Itoa(version[fileName]) + "\n"
			for i:=0; i<len(vms); i++ {
				out += vms[i] + " "
			}
			out = out[:(len(out)-1)]
			version[fileName]++;
		} else {
			//upload new file
			version[fileName] = 1
			out += "1\n"
			vms := getStorePosition()
			m[fileName] = vms		
			for i:=0; i<len(vms); i++ {
				out += vms[i] + " "
			}	
			out = out[:(len(out)-1)]
		}
	} 
	
	//send response
	conn.Write([]byte(out))
	//close connection
	conn.Close()
}



//Main function that starts the server and listens for incoming connections
func main() {

	//pointer = 8
	vm = []string{"1","2","3","4","5","6","7","8","9"}
	m = make(map[string][4]string)
	version = make(map[string]int)
	//assignedMachine := getStorePosition()
	//m["name"] = assignedMachine	
	//fmt.Println(m["name"])

	//get ip address from servers list	
	ip := getIPAddrAndLogfile()
	//ip := "127.0.0.1"
	//listen for incoming connections
	l, err := net.Listen("tcp", ip + ":3000")
	printErr(err, "listening")
	
	//close the listener when app closes
	defer l.Close()
	fmt.Println("Listening on port 3000")

	//Listen for incoming connections
	for {
		conn, err := l.Accept()
		fmt.Println("Accept:", conn.RemoteAddr().String())
		printErr(err, "accepting")

		go parseRequest(conn)
	}
}
	
