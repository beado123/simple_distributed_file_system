package main

import (
	"net"
	"os"
	"io"
	"fmt"
	"bufio"
	"strings"
	"io/ioutil"
	"strconv"
)
var m map[string][]string
var version map[string]int
var pointer int

//membership list of introducer
var lst []string
//a map from machine number to its ip address
var ips map[string]string
//index of current VM
var self string

var acceptMachineAddr *net.UDPAddr
var listnConn *net.UDPConn
var joinMachineNum string

//the log writer that directs output to log file
var logWriter io.Writer

//This function helps printing out errors
func printErr(err error, s string) {
	if err != nil {
		fmt.Println("Error occurs on ", s , "\n" , err.Error())
		os.Exit(1)
	}
}

//This function extracts ip address of current VM from file "ip_address" in current directory
func getIPAddr() string{

	data, err := ioutil.ReadFile("ip_address")
	if err != nil {
		panic(err)
	}

	ip := string(data[:len(data)])
	
	//remove \n from end of line
	if strings.HasSuffix(ip, "\n") {
		ip = ip[:(len(ip) - 1)]
	}
	fmt.Println("ip address of current VM:", ip)
	return ip
}

//This is a helper function that prints the error
func checkErr(err error) {
	if err != nil {
		fmt.Println("Error: ", err)
	}
}

//This function returns the index of process in the membership list
func removeFromList(p string) {
	index := -1
	for i := 0; i < len(lst); i++ {
		if lst[i] == p {
			index = i
		}
	} 
	for i := index; i < len(lst)-1; i++ {
		lst[i] = lst[i+1]
	}
	lst = lst[:len(lst)-1]
}

//This function sends response back to udp packet sender
func writeToPinger(machineNum string, content string) {

	fmt.Fprintln(logWriter, "====function writeToPinger: machineNum", machineNum)
	//write response to newly joined machine
	if machineNum == joinMachineNum {
		
		fmt.Fprintln(logWriter, "write to newly joined machine:", joinMachineNum, " content: ", content)
		_, err := listnConn.WriteToUDP([]byte(content), acceptMachineAddr)
		if err != nil {
			fmt.Fprintf(logWriter, "Couldn't send response %v", err)
		}
	//write updated membership list to other machines
	} else {
		
		fmt.Fprintln(logWriter, "Broadcast to ", machineNum, " content: ", content)
		conn, err := net.Dial("udp", fmt.Sprintf("%s%s", ips[machineNum], ":3333"))
		checkErr(err)
		_, err = conn.Write([]byte(content))
		checkErr(err)
	}
}

//This function broadcasts message to machines in membership list
func broadcast(action string, machine string) {
	fmt.Fprintln(logWriter, "passed in down machine",machine)
	for i := 0; i < len(lst); i++ {
		fmt.Fprintln(logWriter, "lst[i]", lst[i])
		writeToPinger(lst[i], fmt.Sprintf("%s %s", action, machine))
	}
}

//This function assigns each machine in the membership lists their membership lists
func sendMembershipListToPinger() {

	size := len(lst)
	fmt.Fprintln(logWriter, "length of membership list: ", size)
	memLst := "LIST " + self
	if size == 1 {
		writeToPinger(lst[0], memLst)
	} else if size == 2 {
		//1
		writeToPinger(lst[0], fmt.Sprintf("%s %s", memLst, lst[1]))
		//2
		writeToPinger(lst[1], fmt.Sprintf("%s %s", memLst, lst[0]))
	} else if size == 3 {
		//1
		writeToPinger(lst[0], fmt.Sprintf("%s %s", memLst, lst[1]))
		//2
		writeToPinger(lst[1], fmt.Sprintf("%s %s", memLst, lst[2]))
		//3
		writeToPinger(lst[2], fmt.Sprintf("%s %s", memLst, lst[0]))
	} else if size == 4 {
		//1
		writeToPinger(lst[0], fmt.Sprintf("%s %s %s", memLst, lst[1], lst[2]))
		//2
		writeToPinger(lst[1], fmt.Sprintf("%s %s %s", memLst, lst[2], lst[3]))
		//3
		writeToPinger(lst[2], fmt.Sprintf("%s %s %s", memLst, lst[3], lst[0]))
		//4
		writeToPinger(lst[3], fmt.Sprintf("%s %s %s", memLst, lst[0], lst[1]))
	} else if size == 5 {
		//1
		writeToPinger(lst[0], fmt.Sprintf("%s %s %s", memLst, lst[1], lst[2]))
		//2
		writeToPinger(lst[1], fmt.Sprintf("%s %s %s", memLst, lst[2], lst[3]))
		//3
		writeToPinger(lst[2], fmt.Sprintf("%s %s %s", memLst, lst[3], lst[4]))
		//4
		writeToPinger(lst[3], fmt.Sprintf("%s %s %s", memLst, lst[4], lst[0]))
		//5
		writeToPinger(lst[4], fmt.Sprintf("%s %s %s", memLst, lst[0], lst[1]))
	} else if size == 6 {
		//1
		writeToPinger(lst[0], fmt.Sprintf("%s %s %s", memLst, lst[1], lst[2]))
		//2
		writeToPinger(lst[1], fmt.Sprintf("%s %s %s", memLst, lst[2], lst[3]))
		//3
		writeToPinger(lst[2], fmt.Sprintf("%s %s %s", memLst, lst[3], lst[4]))
		//4
		writeToPinger(lst[3], fmt.Sprintf("%s %s %s", memLst, lst[4], lst[5]))
		//5
		writeToPinger(lst[4], fmt.Sprintf("%s %s %s", memLst, lst[5], lst[0]))
		//6
		writeToPinger(lst[5], fmt.Sprintf("%s %s %s", memLst, lst[0], lst[1]))
	} else if size == 7 {
		//1
		writeToPinger(lst[0], fmt.Sprintf("%s %s %s %s", memLst, lst[1], lst[2], lst[3]))
		//2
		writeToPinger(lst[1], fmt.Sprintf("%s %s %s %s", memLst, lst[2], lst[3], lst[4]))
		//3
		writeToPinger(lst[2], fmt.Sprintf("%s %s %s %s", memLst, lst[3], lst[4], lst[5]))
		//4
		writeToPinger(lst[3], fmt.Sprintf("%s %s %s %s", memLst, lst[4], lst[5], lst[6]))
		//5
		writeToPinger(lst[4], fmt.Sprintf("%s %s %s %s", memLst, lst[5], lst[6], lst[0]))
		//6
		writeToPinger(lst[5], fmt.Sprintf("%s %s %s %s", memLst, lst[6], lst[0], lst[1]))
		//7
		writeToPinger(lst[6], fmt.Sprintf("%s %s %s %s", memLst, lst[0], lst[1], lst[2]))
	} else if size == 8 {
		//1
		writeToPinger(lst[0], fmt.Sprintf("%s %s %s %s", memLst, lst[1], lst[2], lst[3]))
		//2
		writeToPinger(lst[1], fmt.Sprintf("%s %s %s %s", memLst, lst[2], lst[3], lst[4]))
		//3
		writeToPinger(lst[2], fmt.Sprintf("%s %s %s %s", memLst, lst[3], lst[4], lst[5]))
		//4
		writeToPinger(lst[3], fmt.Sprintf("%s %s %s %s", memLst, lst[4], lst[5], lst[6]))
		//5
		writeToPinger(lst[4], fmt.Sprintf("%s %s %s %s", memLst, lst[5], lst[6], lst[7]))
		//6
		writeToPinger(lst[5], fmt.Sprintf("%s %s %s %s", memLst, lst[6], lst[7], lst[0]))
		//7
		writeToPinger(lst[6], fmt.Sprintf("%s %s %s %s", memLst, lst[7], lst[0], lst[1]))
		//8
		writeToPinger(lst[7], fmt.Sprintf("%s %s %s %s", memLst, lst[0], lst[1], lst[2]))
	} else if size == 9 {
		//1
		writeToPinger(lst[0], fmt.Sprintf("%s %s %s %s", memLst, lst[1], lst[2], lst[3]))
		//2
		writeToPinger(lst[1], fmt.Sprintf("%s %s %s %s", memLst, lst[2], lst[3], lst[4]))
		//3
		writeToPinger(lst[2], fmt.Sprintf("%s %s %s %s", memLst, lst[3], lst[4], lst[5]))
		//4
		writeToPinger(lst[3], fmt.Sprintf("%s %s %s %s", memLst, lst[4], lst[5], lst[6]))
		//5
		writeToPinger(lst[4], fmt.Sprintf("%s %s %s %s", memLst, lst[5], lst[6], lst[7]))
		//6
		writeToPinger(lst[5], fmt.Sprintf("%s %s %s %s", memLst, lst[6], lst[7], lst[8]))
		//7
		writeToPinger(lst[6], fmt.Sprintf("%s %s %s %s", memLst, lst[7], lst[8], lst[0]))
		//8
		writeToPinger(lst[7], fmt.Sprintf("%s %s %s %s", memLst, lst[8], lst[0], lst[1]))
		//9
		writeToPinger(lst[8], fmt.Sprintf("%s %s %s %s", memLst, lst[0], lst[1], lst[2]))
	}
	
}

//This function responses "ACK" to pinger
func responsePing() {

	fmt.Fprintln(logWriter, "===response ping")
	_, err := listnConn.WriteToUDP([]byte("ACK"), acceptMachineAddr)
	if err != nil {
		fmt.Fprintf(logWriter, "Couldn't send response %v", err)
	}
}

func checkIfExist(machine string) bool{
	for i := 0; i < len(lst); i++ {
		if lst[i] == machine {
			return true;
		}
	}
	return false
}

//This function parses commands and takes action 
func parseUDPRequest(buf []byte, length int) {

	//convert request command into array
	reqArr := strings.Split(string(buf[:length]), " ")
		
	command := reqArr[0]
	machine := reqArr[1]

	remoteIP := strings.Split(string(acceptMachineAddr.String()[:]), ":")
	ips[machine] = remoteIP[0]

	fmt.Fprintf(logWriter, "Parsing request...", command, machine)


	if command == "JOIN" {
		//update membership list
		joinMachineNum = machine
		exist := checkIfExist(machine)
		if exist == true {
			return
		}
		lst = append(lst, machine)
		fmt.Fprintf(logWriter, "====JOIN new member: %s\n", machine)
		fmt.Fprintf(logWriter, "updated membership list:%v\n", lst)
		sendMembershipListToPinger()

	} else if command == "DOWN" {
		joinMachineNum = ""
		exist := checkIfExist(machine)
		if exist == false {
			return
		}
		fmt.Fprintf(logWriter, "====DOWN crashed machine: %s\n", machine)
		fmt.Printf("%s is down\n", machine)
		removeFromList(machine)
		//delete crashed machine from membership list
		reassignFilesToOtherVM(machine)
		sendMembershipListToPinger()
		fmt.Println("updated membership list:",lst)		

	} else if command == "LEAVE" {
		joinMachineNum = ""
		exist := checkIfExist(machine)
		if exist == false {
			return
		}
		fmt.Fprintf(logWriter, "====LEAVE machine: %s\n", machine)
		//delete left machine from membership list
		removeFromList(machine)		
		fmt.Fprintf(logWriter, "%s is leaving\n", machine)
		fmt.Fprintf(logWriter, "updated membership list:%v\n", lst)
		sendMembershipListToPinger()
		broadcast("LEAVE", machine)

	} else if command == "PING" {
		joinMachineNum = ""
		responsePing()
	} 
}

//Main function that starts the server and listens for incoming UDP packets
func startIntroducer() {

	
	//create local log file for debugging
	file, err := os.Create("logger")
	checkErr(err)
	logWriter = io.MultiWriter(file)

	fmt.Println("===starting introducer")

	//get ip address from servers list	
	ip := getIPAddr()
	self = ip[15:17]

	//initialize ip map (num => ip)
	ips = make(map[string]string)

	addr := net.UDPAddr{
		Port: 3333,
		IP: net.ParseIP(ip),
	}
	
	/* Now listen at selected port */
    ser, err := net.ListenUDP("udp", &addr)
	listnConn = ser

    checkErr(err)
    defer ser.Close()

	fmt.Fprintln(logWriter, "Listening on port 3456")

	//Listen for incoming connections
	buf := make([]byte, 1024)

    for {
        n, remoteAddr, err := listnConn.ReadFromUDP(buf)
		fmt.Fprintf(logWriter, "=============\nReceived a message from %v:%s \n", remoteAddr, string(buf[:n]))
		checkErr(err)
		
		acceptMachineAddr = remoteAddr
		parseUDPRequest(buf, n)        
   }
}

func reassignFilesToOtherVM(machine string) {

	fileArr := []string{}
	for file,_ := range m {
		for _,vm := range m[file] {
			if vm == machine {
				fileArr = append(fileArr, file)
			}
		}
	}
	fmt.Printf("files in crashed machine: %#v\n", fileArr)
	
	for index:=0; index<len(fileArr); index++ {
		oneFile := fileArr[index]
		fmt.Println("vm group of", oneFile, m[oneFile])

		//remove crashed machine from m[oneFile]
		machineIndex := -1
		for i:=0; i<len(m[oneFile]); i++ {
			if m[oneFile][i] == machine {
				machineIndex = i
			}
		}
		m[oneFile] = append(m[oneFile][:machineIndex], m[oneFile][machineIndex+1:]...)
		fmt.Println("vm froup after removing", oneFile,  m[oneFile])

		//find vm other than VMs in m[oneFile]
		newVm := -1
		fmt.Println("lst", lst)
		for i:=0; i<len(lst); i++ {
			foundSame := false
			for j:=0; j<len(m[oneFile]); j++ {
				fmt.Print(lst[i], m[oneFile][j], " ")
				if lst[i] == m[oneFile][j] {
					foundSame = true
				}
			}
			if foundSame == false {
				newVm = i
				break
			}
		}
		fmt.Print("\n")
		fmt.Println("newVm", newVm)
		//append new VM to file vm group
		m[oneFile] = append(m[oneFile], lst[newVm])
		conn, err := net.Dial("tcp", fmt.Sprintf("%s%s%s", "fa18-cs425-g69-", m[oneFile][0], ".cs.illinois.edu:6666"))
		checkErr(err)
		_, err = conn.Write([]byte("failfail"))
		_, err = conn.Write([]byte(oneFile + "\n" + lst[newVm]))
		checkErr(err)
	}
}


func getStorePosition() [4]string{
	n := len(lst)
	arr := [4]string{}
	fmt.Println(pointer,lst)
	if pointer + 1 < n {
		arr[0] = lst[pointer+1]
	} else {arr[0] = lst[0]}
	if pointer + 2 < n {
		arr[1] = lst[pointer+2]
	} else {arr[1] = lst[(pointer+2-n)]}
	if pointer + 3 < n {
		arr[2] = lst[pointer+3]
	} else {arr[2] = lst[pointer+3-n]}
	if pointer + 4 < n {
		arr[3] = lst[pointer+4]	
		pointer += 4
	} else {
		arr[3] = lst[pointer+4-n]
		pointer = (pointer+4) - n
	}
	return arr
}
func parseRequest(conn net.Conn) {

	//create a buffer to hold transferred data and read incoming data into buffer
	buf := make([]byte, 1024)
	reqLen, err := conn.Read(buf)
	printErr(err, "reading")

	//convert request command into array
	reqArr := strings.Split(string(buf[:reqLen]), " ")
	
	for i:=0; i<len(reqArr); i++ {
		fmt.Print(reqArr[i], " ")
	}
	fmt.Print("\n")

	cmd := reqArr[0]
	out := ""
	if cmd == "put"{
		//"put localfilename sdfsfilename"
		fileName := reqArr[2]
		fileName = fileName[:len(fileName)-1]
		fmt.Println("filename", m[fileName])
		_, ok := m[fileName]
		if ok && m[fileName] != nil{
			vms := m[fileName]
			version[fileName]++;
			out += strconv.Itoa(version[fileName]) + "\n"
			for i:=0; i<len(vms); i++ {
				out += vms[i] + " "
			}
			out = out[:(len(out)-1)]			
		} else {
			//upload new file
			version[fileName] = 1
			out += "1\n"
			vms := getStorePosition()
			
			for i:=0; i<len(vms); i++ {
				m[fileName] = append(m[fileName], vms[i])
			}	
			fmt.Println("assign vm after", m[fileName])
			fmt.Println(m[fileName][0])	
			for i:=0; i<len(vms); i++ {
				out += vms[i] + " "
			}	
			out = out[:(len(out)-1)]
		}
	} else if cmd == "get" {
		//"get sdfsfilename localfilename"
		fileName := reqArr[1]
		fmt.Println("fileName", fileName)
		_, ok := m[fileName]
		if ok && m[fileName] != nil{
			vms := m[fileName]
			out += strconv.Itoa(version[fileName]) + "\n"
			out += vms[0]
		} else {
			fmt.Println("File", fileName, "does not Exist!")
			out = "NOTFOUND\nNOTFOUND"
		}	

	} else if cmd == "ls" {
		//"ls sdfsfilename"
		fileName := reqArr[1]
		fileName = fileName[:(len(fileName)-1)]
		fmt.Println("ls", m[fileName])
		_, ok := m[fileName]
		if ok && m[fileName] != nil {
			vms := m[fileName]
			for i:=0; i<len(vms); i++ {
				out += vms[i] + " "
			}
			out = out[:(len(out)-1)]
		} else {
			fmt.Println("File", fileName, "does not exist!")
			out = "NOTFOUND"
		}	

	} else if cmd == "delete" {
		//"delete sdfsfilename"
		fileName := reqArr[1]
		fileName = fileName[:(len(fileName)-1)]
		fmt.Println(m[fileName])
		_, ok := m[fileName]
		if ok {
			vms := m[fileName]
			for i:=0; i<len(vms); i++ {
				out += vms[i] + " "
			}
			out  = out[:(len(out)-1)]
			m[fileName] = nil
			version[fileName] = -1
		} else {
			out = "NOTFOUND"
			fmt.Println("File", fileName, "does not exist!") 
		}

	} else if cmd == "get-versions" {
		//"get-versions sdfsfilename num-versions localfilename"
		//return version-num1 version-num2\nvm1 vm2
		fileName := reqArr[1]
		numVersion, err := strconv.Atoi(reqArr[2])	
		if err != nil {
			fmt.Println(err)
		}
		currVersion := version[fileName]
		for i:=0; i<numVersion; i++ {
			out += strconv.Itoa(currVersion-i) + " "
		}
		out = out[:(len(out) -1)]
		_, ok := m[fileName]
		if ok {
			out += "\n" + m[fileName][0]
		} else {
			fmt.Println("File", fileName, "does not exist!")
		}
	}

	fmt.Println("Write back to worker",out)
	//send response
	conn.Write([]byte(out))
	//close connection
	conn.Close()
}
func startMaster() {

	pointer = -1
	m = make(map[string][]string)
	version = make(map[string]int)

	//get ip address from servers list	
	ip := getIPAddr()
	//listen for incoming connections
	l, err := net.Listen("tcp", ip + ":6666")
	printErr(err, "listening")
	
	//close the listener when app closes
	defer l.Close()
	fmt.Println("Listening on port 5678")

	//Listen for incoming connections
	for {
		conn, err := l.Accept()
		fmt.Println("TCP Accept:", conn.RemoteAddr().String())
		printErr(err, "accepting")

		go parseRequest(conn)
	}
}
func main() {
	
	for true {
		buf := bufio.NewReader(os.Stdin)
		input, err := buf.ReadBytes('\n')
		if err != nil {
		    fmt.Println(err)
		} else {
			cmd := string(input)
			if strings.Contains(cmd, "JOIN") {
				go startIntroducer()
				go startMaster()

			} else if strings.Contains(cmd, "LIST") {
				fmt.Print("Membership list: [", self, " ")
				for i := 0; i < len(lst); i++ {
					if i < len(lst) -1 {
						fmt.Print(lst[i], " ")
					} else {
						fmt.Print(lst[i])
					}
				}
				fmt.Println("]")

			} else if strings.Contains(cmd, "SELF"){
				fmt.Println("Self ID:", self)
			} else if strings.Contains(cmd, "LEAVE") {
				break

			} else {
				fmt.Println("Input does not match any commads!")
			}
		}
	}
}
	
