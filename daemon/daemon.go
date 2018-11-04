package daemon

import (
    "fmt"
    "net"
    "time"
    "strconv"
    "strings"
    "io/ioutil"
    "sync"
    "io"
    "os"
)

const BUFFERSIZE = 1024

type Node struct {
	Id string
	State int
	T time.Time
}

type Daemon struct {
	VmId string
	VmIpAddress string
	Ln net.Listener
	PortTCP string
	Ser *net.UDPConn
	PortUDP string
	MembershipList map[string]*Node
	IsActive bool
	Master string
	MyMutex *sync.Mutex
}

func NewDaemon(id string) (d *Daemon, err error) {
	ip_address := getIPAddrAndLogfile()
	vm_id := ip_address[15:17]
	l, err := net.Listen("tcp", ip_address + ":6666")
	if err != nil {
		fmt.Println(err)
                return
	}
	addr := net.UDPAddr{
        	Port: 3333,
        	IP: net.ParseIP(ip_address),
    	}
	ser, err := net.ListenUDP("udp", &addr)
	if err != nil {
        	fmt.Println(err)
        	return
    	}
	master := "fa18-cs425-g69-" + id + ".cs.illinois.edu"
	mutex := &sync.Mutex{}
	d = &Daemon {
		VmId: vm_id,
		VmIpAddress: ip_address,
		Ln: l,
		PortTCP: "6666",
		Ser: ser,
		PortUDP: "3333",
		MembershipList: make(map[string]*Node),
		IsActive: true,
		Master: master,
		MyMutex: mutex,
	}
	return d, err
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

func fillString(retunString string, toLength int) string {
	for {
		lengtString := len(retunString)
		if lengtString < toLength {
			retunString = retunString + ":"
			continue
		}
		break
	}
	return retunString
}

func (self *Daemon) DaemonListenTCP() {
	if self.IsActive == false {
		return
	}
	
	//listen for incoming connections
	for true {
		conn, err := self.Ln.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		go self.ParseRequest(conn)
	}
}

func (self *Daemon) ParseRequest(conn net.Conn) {
	bufferRequest := make([]byte, 8)
	conn.Read(bufferRequest)
	request := string(bufferRequest)
	if request == "put_file" {
		self.ReceivePutRequest(conn)
	} else if request == "get_file" {
		self.ReceiveGetRequest(conn)
	} else if request == "del_file" {
		self.ReceiveDeleteRequest(conn)
	} else if request == "get_vers" {
		self.ReceiveGetVersionRequest(conn)
	} else if request == "mdzzmdzz" {
		self.ReceiveReplicateRequestFromWorker(conn)
	} else if request == "failfail" {
		self.ReceiveReplicateRequestFromMaster(conn)
	}
}

func (self *Daemon) ReceivePutRequest(conn net.Conn) {
	defer conn.Close()
	//read file size and file name first
	bufferFileName := make([]byte, 64)
	bufferFileSize := make([]byte, 10)
	conn.Read(bufferFileSize)
	fileSize, _ := strconv.ParseInt(strings.Trim(string(bufferFileSize), ":"), 10, 64)
	conn.Read(bufferFileName)
	fileName := strings.Trim(string(bufferFileName), ":")
	fullPath := "sdfs/" + fileName
	
	//create new file
	newFile, err := os.Create(fullPath)
	if err != nil {
		fmt.Println(err)
	}
	defer newFile.Close()
	var receivedBytes int64
	for true {
		if (fileSize - receivedBytes) < BUFFERSIZE {
			io.CopyN(newFile, conn, (fileSize - receivedBytes))
			conn.Read(make([]byte, (receivedBytes+BUFFERSIZE)-fileSize))
			break
		}
		io.CopyN(newFile, conn, BUFFERSIZE)
		receivedBytes += BUFFERSIZE
	}
	fmt.Println("Received file completely!")
	response := "putACK"
	conn.Write([]byte(response))
}

func (self *Daemon) PutHelper(cmd string) (num string, ids []string) {
	//connect to master
        conn, err := net.Dial("tcp", self.Master + ":" + self.PortTCP)
        if err != nil {
                fmt.Println(err)
                return
        }
	defer conn.Close()

        //send to socket
        fmt.Fprintf(conn, cmd)

        //read message from socket
        buf := make([]byte, BUFFERSIZE)
        reqLen, err := conn.Read(buf)
        if err != nil {
                fmt.Println(err)
                return
        }
        reqArr := strings.Split(string(buf[:reqLen]), "\n")
	num = reqArr[0]
	ids = strings.Split(reqArr[1], " ")
	return
} 

func (self *Daemon) SendPutRequest(cmd string) {
	start := time.Now()
	num, reqArr := self.PutHelper(cmd)
	//connect to each replica host
	var wg sync.WaitGroup
	var count int = 0
	wg.Add(len(reqArr))
	for _, id := range reqArr {
		go func(id string, cmd string, num string) {
			localFileName, sdfsFileName := ParsePutRequest(cmd)
                        fileName := num + "_" + sdfsFileName
			localFullPath := "local/" + localFileName
			sdfsFullPath := "sdfs/" + fileName

			if id == self.VmId {
				//move local file to sdfs
				err := FileCopy(localFullPath, sdfsFullPath)
				if err == nil {
					count += 1
				}
				wg.Done()
				return
			}

			name := "fa18-cs425-g69-" + id + ".cs.illinois.edu"
			conn, err := net.Dial("tcp", name + ":" + self.PortTCP)
			if err != nil {
                		fmt.Println(err)
				wg.Done()
                		return
        		}
			defer conn.Close()

			//read from localfile
			request := "put_file"
			file, err := os.Open(localFullPath)
			if err != nil {
				fmt.Println(err)
				wg.Done()
				return
			}
			fileInfo, err := file.Stat()
			if err != nil {
				fmt.Println(err)
				wg.Done()
				return
			}
			fileSize := fillString(strconv.FormatInt(fileInfo.Size(), 10), 10)
			fileName = fillString(fileName, 64)
			conn.Write([]byte(request))
			conn.Write([]byte(fileSize))
			conn.Write([]byte(fileName))
			sendBuffer := make([]byte, BUFFERSIZE)
			for true{
				_, err = file.Read(sendBuffer)
				if err == io.EOF {
					break
				}
				conn.Write(sendBuffer)
			}

			//receive putACK from replica
			buf := make([]byte, 64)
		        reqLen, err := conn.Read(buf)
        		if err != nil {
                		fmt.Println(err)
				wg.Done()
                		return
        		}
			response := string(buf[:reqLen])
			if response == "putACK" {
				count += 1
			}
			wg.Done()
		}(id, cmd, num)
	}	
	wg.Wait()
	end := time.Now()	
	elipsed := end.Sub(start)
	fmt.Println("insert time: ", elipsed)
	
	//check if receive all putACK
	if count == len(reqArr) {
		fmt.Println("put successfully!")
	} else {
		fmt.Println("put fail!")
	}
}

//This function receives sdfs file name and return the file size and the file content
func (self *Daemon) ReceiveGetRequest(conn net.Conn) {
	defer conn.Close()
	//find file name
	bufferFileName := make([]byte, 64)
        conn.Read(bufferFileName)
        fileName := strings.Trim(string(bufferFileName), ":")
	fullPath := "sdfs/" + fileName

	//read file
	file, err := os.Open(fullPath)
        if err != nil {
        	fmt.Println(err)
               	return
        }
        fileInfo, err := file.Stat()
        if err != nil {
        	fmt.Println(err)
                return
       	}
	fmt.Println(fileInfo.Size())
        fileSize := fillString(strconv.FormatInt(fileInfo.Size(), 10), 10)
	conn.Write([]byte(fileSize))
	sendBuffer := make([]byte, BUFFERSIZE)
        for true{
        	_, err = file.Read(sendBuffer)
                if err == io.EOF {
                	break
                }
                conn.Write(sendBuffer)
        }
}

func (self *Daemon) GetHelper(cmd string) (version string, id string){
	//connect to master
        conn, err := net.Dial("tcp", self.Master + ":" + self.PortTCP)
        if err != nil {
                fmt.Println(err)
                return
        }

        //send to socket
        fmt.Fprintf(conn, cmd)

        //read message from socket
        buf := make([]byte, BUFFERSIZE)
        reqLen, err := conn.Read(buf)
        if err != nil {
                fmt.Println(err)
                return
        }
	reqArr := strings.Split(string(buf[:reqLen]), "\n")
	version = reqArr[0]
	id = reqArr[1]
	return
}

func (self *Daemon) SendGetRequest(cmd string) {
	start := time.Now()
	version, id := self.GetHelper(cmd)
	if version == "NOTFOUND" {
		fmt.Println("The file is not available!")
		return
	}
	//send put request
	localFileName, sdfsFileName := ParseGetRequest(cmd)
        localFullPath := "local/" + localFileName
	fileName := version + "_" + sdfsFileName
        sdfsFullPath := "sdfs/" + fileName
	if self.VmId == id {
               	FileCopy(sdfsFullPath, localFullPath)
		return 
	}
	name := "fa18-cs425-g69-" + id + ".cs.illinois.edu"
        conn, err := net.Dial("tcp", name + ":" + self.PortTCP)
        if err != nil {
        	fmt.Println(err)
        	return
        }
        defer conn.Close()	
	request := "get_file"
	conn.Write([]byte(request))
	fileName = fillString(fileName, 64)
	conn.Write([]byte(fileName))

	//receive new file
	bufferFileSize := make([]byte, 10)
        conn.Read(bufferFileSize)
        fileSize, _ := strconv.ParseInt(strings.Trim(string(bufferFileSize), ":"), 10, 64)
	//fmt.Println(fileSize)
	newFile, err := os.Create(localFullPath)
        if err != nil {
        	fmt.Println(err)
        }
        defer newFile.Close()
        var receivedBytes int64
        for true {
                if (fileSize - receivedBytes) < BUFFERSIZE {
                        io.CopyN(newFile, conn, (fileSize - receivedBytes))
                        conn.Read(make([]byte, (receivedBytes+BUFFERSIZE)-fileSize))
                        break
                }
                io.CopyN(newFile, conn, BUFFERSIZE)
                receivedBytes += BUFFERSIZE
        }
	fmt.Println("Received file completely!")
	end := time.Now()	
	elipsed := end.Sub(start)
	fmt.Println("read time: ", elipsed)
}

func (self *Daemon) ReceiveDeleteRequest(conn net.Conn) {
	defer conn.Close()
	bufferFileName := make([]byte, 64)
	conn.Read(bufferFileName)
	fileName := strings.Trim(string(bufferFileName), ":")
	DeleteSdfsfile(fileName)
	response := "deleteACK"
	conn.Write([]byte(response))
}

func (self *Daemon) DeleteHelper(cmd string) (reqArr []string) {
	//connect to master
        conn, err := net.Dial("tcp", self.Master + ":" + self.PortTCP)
        if err != nil {
                fmt.Println(err)
                return
        }
	defer conn.Close()

        //send to socket
        fmt.Fprintf(conn, cmd)

        //read message from socket
        buf := make([]byte, BUFFERSIZE)
        reqLen, err := conn.Read(buf)
        if err != nil {
                fmt.Println(err)
                return
        }
        reqArr = strings.Split(string(buf[:reqLen]), " ")
        return
}

func (self *Daemon) SendDeleteRequest(cmd string) {
	reqArr := self.DeleteHelper(cmd)
	var wg sync.WaitGroup
	var count int = 0
        wg.Add(len(reqArr))
        for _, id := range reqArr {
                go func(id string, cmd string) {
			sdfsFileName := ParseDeleteRequest(cmd)
			if id == self.VmId {
				DeleteSdfsfile(sdfsFileName)
				count += 1
				wg.Done()
				return
			}

			name := "fa18-cs425-g69-" + id + ".cs.illinois.edu"
                        conn, err := net.Dial("tcp", name + ":" + self.PortTCP)
                        if err != nil {
                                fmt.Println(err)
                                wg.Done()
                                return
                        }
                        defer conn.Close()
			request := "del_file"
			conn.Write([]byte(request))
			fileName := fillString(sdfsFileName, 64)
                        conn.Write([]byte(fileName))
			
			//receive deleteACK from replica
			buf := make([]byte, 64)
		        reqLen, err := conn.Read(buf)
        		if err != nil {
                		fmt.Println(err)
				wg.Done()
                		return
        		}
			response := string(buf[:reqLen])
			if response == "deleteACK" {
				count += 1
			}			
			wg.Done()
                }(id, cmd)
        }
        wg.Wait()
	
	//check if receive all deleteACK
	if count == len(reqArr) {
		fmt.Println("delete successfully!")
	} else {
		fmt.Println("delete fail!")
	}
}

func (self *Daemon) SendLsRequest(cmd string) {
	//connect to master
        conn, err := net.Dial("tcp", self.Master + ":" + self.PortTCP)
        if err != nil {
                fmt.Println(err)
                return
        }
	defer conn.Close()

        //send to socket
        fmt.Fprintf(conn, cmd)

        //read message from socket
        buf := make([]byte, 64)
        reqLen, err := conn.Read(buf)
        if err != nil {
                fmt.Println(err)
                return
        }
        reqArr := strings.Split(string(buf[:reqLen]), " ")
	if reqArr[0] == "NOTFOUND" {
		return
	}
	for _, id := range reqArr {
		name := "fa18-cs425-g69-" + id + ".cs.illinois.edu"
		fmt.Println(name)
	}
}

func (self *Daemon) StoreRequest() {
	files, err := ioutil.ReadDir("sdfs")
    	if err != nil {
        	fmt.Println(err)
		return
    	}
	//m := make(map[string]int)
	fmt.Println("sdfs:")
    	for _, f := range files {
		fmt.Println(f.Name())
		/*reqArr := strings.Split(f.Name(), "_")
		if _, ok := m[reqArr[1]]; ok {
			
		} else {
			m[reqArr[1]] = 0
			fmt.Println(reqArr[1])
		}*/
    	}

	files, err = ioutil.ReadDir("local")
        if err != nil {
                fmt.Println(err)
                return
        }
	if len(files) == 0 {
		return
	}
        //n := make(map[string]int)
	fmt.Println("local:")
        for _, f := range files {
		fmt.Println(f.Name())
                //reqArr := strings.Split(f.Name(), "_")
                /*if _, ok := n[f.Name()]; ok {

                } else {
                        n[f.Name()] = 0
                        fmt.Println(f.Name())
                }*/
        }
}

func (self *Daemon) ReceiveGetVersionRequest(conn net.Conn) {
	defer conn.Close()
	bufferFileName := make([]byte, BUFFERSIZE)
        conn.Read(bufferFileName)
        fileNames := strings.Split(strings.Trim(string(bufferFileName), ":"), " ")
	
	//read file
	for _, fileName := range fileNames {
		fullPath := "sdfs/" + fileName
		file, err := os.Open(fullPath)
	        if err != nil {
        		fmt.Println(err)
               		return
        	}
        	fileInfo, err := file.Stat()
        	if err != nil {
        		fmt.Println(err)
                	return
       		}
        	fileSize := fillString(strconv.FormatInt(fileInfo.Size(), 10), 10)
		conn.Write([]byte(fileSize))
		sendBuffer := make([]byte, BUFFERSIZE)
        	for true{
        		_, err = file.Read(sendBuffer)
                	if err == io.EOF {
                		break
                	}
                	conn.Write(sendBuffer)
        	}
	}
}

func (self *Daemon) GetVersionHelper(cmd string) (versions []string, id string) {
	//connect to master
        conn, err := net.Dial("tcp", self.Master + ":" + self.PortTCP)
        if err != nil {
                fmt.Println(err)
                return
        }
	defer conn.Close()

        //send to socket
        fmt.Fprintf(conn, cmd)

        //read message from socket
        buf := make([]byte, BUFFERSIZE)
        reqLen, err := conn.Read(buf)
        if err != nil {
                fmt.Println(err)
		return 
	}
	reqArr := strings.Split(string(buf[:reqLen]), "\n")
	versions = strings.Split(reqArr[0], " ")
	id = reqArr[1]
	return
}

func (self *Daemon) SendGetVersionRequest(cmd string) {
	start := time.Now()
	versions, id := self.GetVersionHelper(cmd)
	localFileName, sdfsFileName, _ := ParseGetVersionRequest(cmd)
	localFullPath := "local/" + localFileName
	//fmt.Println(localFullPath)
	fileName := ""
	for i, version := range versions {
		if i == len(versions) - 1 {
			fileName += version + "_" + sdfsFileName
		} else {
			fileName += version + "_" + sdfsFileName + " "
		}	
	}

	if self.VmId == id {	
		FileCopyToOne(localFullPath, sdfsFileName, versions)	
		return
	}

	name := "fa18-cs425-g69-" + id + ".cs.illinois.edu"
        conn, err := net.Dial("tcp", name + ":" + self.PortTCP)
        if err != nil {
                fmt.Println(err)
                return
        }
        defer conn.Close()
        request := "get_vers"
        conn.Write([]byte(request))
	fileName = fillString(fileName, BUFFERSIZE)
        conn.Write([]byte(fileName))

	//create new file
	newFile, err := os.Create(localFullPath)
        if err != nil {
        	fmt.Println(err)
        }
        defer newFile.Close()
	//fileNames := strings.Split(fileName, " ")
	for _, version := range versions {
		bufferFileSize := make([]byte, 10)
		conn.Read(bufferFileSize)
		fileSize, _ := strconv.ParseInt(strings.Trim(string(bufferFileSize), ":"), 10, 64)
		var receivedBytes int64
		newFile.WriteString(version)
		newFile.WriteString("\n")
		for true{
			if (fileSize - receivedBytes) < BUFFERSIZE {
				io.CopyN(newFile, conn, (fileSize - receivedBytes))
				conn.Read(make([]byte, (receivedBytes+BUFFERSIZE)-fileSize))
				break
			}
			io.CopyN(newFile, conn, BUFFERSIZE)
			receivedBytes += BUFFERSIZE
		}
		newFile.WriteString("\n")
	}
	fmt.Println("Received latest file completely!")
	end := time.Now()	
	elipsed := end.Sub(start)
	fmt.Println("get-versions time: ", elipsed)
}

//re-replicate
func (self *Daemon) ReceiveReplicateRequestFromMaster(conn net.Conn) {
	buffer := make([]byte, BUFFERSIZE)
	reqLen, _ := conn.Read(buffer)
	reqArr := strings.Split(string(buffer[:reqLen]), "\n")
	name := reqArr[0]
	id := reqArr[1]

	//set up connection with id VM
	conn, err := net.Dial("tcp", "fa18-cs425-g69-" + id + ".cs.illinois.edu:" + self.PortTCP)
        if err != nil {
                fmt.Println(err)
                return
        }
	defer conn.Close()

	request := "mdzzmdzz"
	conn.Write([]byte(request))
	//file transfer
	files,_ := ioutil.ReadDir("sdfs")
	for _, file := range files {
		if strings.HasSuffix(file.Name(), name) == false {
			continue
		}
		fullPath := "sdfs/" + file.Name()
		fmt.Println(fullPath)
		file, err := os.Open(fullPath)
	        if err != nil {
        		fmt.Println(err)
               		return
        	}
        	fileInfo, err := file.Stat()
        	if err != nil {
        		fmt.Println(err)
                	return
       		}
		fileName := fillString(fullPath, 64)
		conn.Write([]byte(fileName))
		fileSize := fillString(strconv.FormatInt(fileInfo.Size(), 10), 10)
		conn.Write([]byte(fileSize))
		sendBuffer := make([]byte, BUFFERSIZE)
        	for true{
        		_, err = file.Read(sendBuffer)
                	if err == io.EOF {
                		break
                	}
                	conn.Write(sendBuffer)
        	}
	}	
}

func (self *Daemon) ReceiveReplicateRequestFromWorker(conn net.Conn) {
	defer conn.Close()
	for true {
		bufferFileName := make([]byte, 64)
        	_, err := conn.Read(bufferFileName)
		if err == io.EOF {
			break
		}
        	fileName := strings.Trim(string(bufferFileName), ":")
		//fullPath := "sdfs/" + fileName
		fullPath := fileName
		fmt.Println(fullPath)
		bufferFileSize := make([]byte, 10)
		conn.Read(bufferFileSize)
		fileSize, _ := strconv.ParseInt(strings.Trim(string(bufferFileSize), ":"), 10, 64)

		//create new file
		newFile, err := os.Create(fullPath)
        	if err != nil {
        		fmt.Println(err)
        	}
        	defer newFile.Close()
		var receivedBytes int64
		for true{
			if (fileSize - receivedBytes) < BUFFERSIZE {
				io.CopyN(newFile, conn, (fileSize - receivedBytes))
				conn.Read(make([]byte, (receivedBytes+BUFFERSIZE)-fileSize))
				break
			}
			io.CopyN(newFile, conn, BUFFERSIZE)
			receivedBytes += BUFFERSIZE
		}
	}
}

////////////////////helper function////////////////////////////////////////////////
func FileCopy(source string, destination string) error{
	fmt.Println(source)
	fmt.Println(destination)
	from, err := os.Open(source)
  	if err != nil {
    		fmt.Println(err)
		return err
  	}
  	defer from.Close()

  	to, err := os.Create(destination)
  	if err != nil {
    		fmt.Println(err)
		return err
  	}
  	defer to.Close()

  	_, err = io.Copy(to, from)
  	if err != nil {
    		fmt.Println(err)
		return err
  	}
	return err
}

func FileCopyToOne(localFullPath string, sdfsFileName string, versions []string) {
	//create new file
	newFile, err := os.Create(localFullPath)
        if err != nil {
        	fmt.Println(err)
        }
        newFile.Close()

	for _, version := range versions {
		fileName := version + "_" + sdfsFileName	
		sdfsFullPath := "sdfs/" + fileName
		from, err := os.Open(sdfsFullPath)
	        if err != nil {
        	        fmt.Println(err)
               		return
        	}
        	defer from.Close()
		to, err := os.OpenFile(localFullPath, os.O_APPEND|os.O_WRONLY, 0666)
		if err != nil {
			fmt.Println(err)
			return
		}
		defer to.Close()
		to.WriteString(version)
		to.WriteString("\n")
		_, err = io.Copy(to, from)
	        if err != nil {
        	        fmt.Println(err)
                	return
        	}
		to.WriteString("\n")
	}	
}

func ParsePutRequest(cmd string) (localFileName string, sdfsFileName string) {
	if strings.HasSuffix(cmd, "\n") {
		cmd = cmd[:(len(cmd) - 1)]
	}
	reqArr := strings.Split(cmd, " ")
        localFileName = reqArr[1]
        sdfsFileName = reqArr[2]
	return 
}

func ParseGetRequest(cmd string) (localFileName string, sdfsFileName string) {
	if strings.HasSuffix(cmd, "\n") {
                cmd = cmd[:(len(cmd) - 1)]
        }
        reqArr := strings.Split(cmd, " ")
        localFileName = reqArr[2]
        sdfsFileName = reqArr[1]
        return
}

func ParseDeleteRequest(cmd string) (sdfsFileName string) {
	if strings.HasSuffix(cmd, "\n") {
                cmd = cmd[:(len(cmd) - 1)]
        }
	reqArr := strings.Split(cmd, " ")
	sdfsFileName = reqArr[1]
	return
}

func ParseGetVersionRequest(cmd string) (localFileName string, sdfsFileName string, num string) {
	if strings.HasSuffix(cmd, "\n") {
                cmd = cmd[:(len(cmd) - 1)]
        }
        reqArr := strings.Split(cmd, " ")
        localFileName = reqArr[3]
        sdfsFileName = reqArr[1]
	num = reqArr[2]
        return
}

func DeleteSdfsfile(fileName string) {
	files,_ := ioutil.ReadDir("sdfs")
        for _, file := range files {
                if strings.HasSuffix(file.Name(), fileName) == true {
			fullPath := "sdfs/" + file.Name()
			err := os.Remove(fullPath)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
}

func (self *Daemon) CleanOutSdfs() {
	if _, err := os.Stat("sdfs"); os.IsNotExist(err) {
    		os.Mkdir("sdfs", 0777)
	} else {
		files,_ := ioutil.ReadDir("sdfs")
	        for _, file := range files {
			fullPath := "sdfs/" + file.Name()
			err := os.Remove(fullPath)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
}

////////////////////////////mp2 function///////////////////////////////
func (self *Daemon) DaemonListenUDP() {
	if self.IsActive == false {
		return
	}
	buf := make([]byte, 1024)
	for true {
		reqLen, client_addr, err := self.Ser.ReadFromUDP(buf)
		if err != nil {
			fmt.Println(err)
			return
		}
		reqArr := strings.Split(string(buf[:reqLen]), " ")	
		if reqArr[0] == "PING" {
			go self.ResponsePING(client_addr, reqArr)			
		} else if reqArr[0] == "LIST" {
			go self.ResponseLIST(reqArr)
		}	
	}
}

//This function sends ACK back to pinger
func (self *Daemon) ResponsePING(addr *net.UDPAddr, reqArr []string) {
	_, err := self.Ser.WriteToUDP([]byte("ACK"), addr)
	if err != nil { 
		fmt.Println(err)
	}
}

//This function updates timestamp in membership list
func (self *Daemon) ResponseACK(conn net.Conn, id string) {
	buf := make([]byte, 1024)
	reqLen, err := conn.Read(buf)
	if err != nil {
                fmt.Println(err)
                return
        }
	reqArr := strings.Split(string(buf[:reqLen]), " ")
	if reqArr[0] == "ACK" {
		if _, ok := self.MembershipList[id]; ok {
			self.MembershipList[id].T = time.Now()
		}
	}
	conn.Close()
}

//This function adds new member into membership list
func (self *Daemon) AddNewMember (id string) (member *Node) {
	member = new(Node)
	member.Id = id
	member.State = 1
	member.T = time.Now()
	self.MembershipList[id] = member
	return
}

//This function updates membership list
func (self *Daemon) ResponseLIST(reqArr []string) {
	self.MyMutex.Lock()
	for id, _ := range self.MembershipList {
		self.MembershipList[id].State = 0	
	}
	for i, id := range reqArr {
        	if i == 0 {
                	continue
                }
                check := 0
		if _, ok := self.MembershipList[id]; ok {
			self.MembershipList[id].State = 1
                        self.MembershipList[id].T = time.Now()
			check = 1
                }
                if check == 0 {
                	self.AddNewMember(id)
                }
        }
	self.MyMutex.Unlock()
}

//This function tells introducer failure machine
func (self *Daemon) SendDOWN(id string) {
	conn, err := net.Dial("udp", self.Master + ":" + self.PortUDP)
        if err != nil {
                fmt.Println(err)
                return
        }
        msg := "DOWN " + id
	down_buffer := []byte(msg)
        _, err = conn.Write(down_buffer)
        if err != nil {
                fmt.Println(err)
                return
        }
	conn.Close()
}

//This function pings to members in membership list
func (self *Daemon) PingToMembers() {
	if self.IsActive == false {
                return
        }
	for true {
		for _, curr_node := range self.MembershipList {
			self.MyMutex.Lock()
			if curr_node.State == 0 {
				self.MyMutex.Unlock()
				continue;
			}
			self.MyMutex.Unlock()
			member_address := "fa18-cs425-g69-" + curr_node.Id + ".cs.illinois.edu:" + self.PortUDP
			conn, err := net.Dial("udp", member_address)
			if err != nil {
                        	fmt.Println(err)
                        	continue
                        }
			msg := "PING " + self.VmId
			buf := []byte(msg)
			_, err = conn.Write(buf)
			if err != nil {
				fmt.Println(err)
				continue
			}
			go self.ResponseACK(conn, curr_node.Id)		
			
		}
		time.Sleep(time.Millisecond * 500)
	}
}

//This function print the membership list
func (self *Daemon) PrintMembershipList() {
	fmt.Print("Membership list: [", " ")
	for id, _ := range self.MembershipList {
		if self.MembershipList[id].State == 1 {
			fmt.Print(id, " ")
		}
	}
	fmt.Println("]")
}

//This function checks if any members in membership list time out
func (self *Daemon) TimeOutCheck() {
	if self.IsActive == false {
                return
        }
	for true {
		for id, curr_node := range self.MembershipList {
			self.MyMutex.Lock()
			if curr_node.State == 0 {
				self.MyMutex.Unlock()
				continue;
			} else {	
				self.MyMutex.Unlock()
				elipsed := time.Now().Sub(curr_node.T).Seconds()
				if elipsed > 0.75 {
					self.MyMutex.Lock()
					self.MembershipList[id].State = 0	
					self.MyMutex.Unlock()
					go self.SendDOWN(id)
				}
			}
		}
		time.Sleep(time.Millisecond * 500)
	}
}

//This function lets VM join the group
func (self *Daemon) JoinGroup () (reqArr []string, err error) {
	buf := make([]byte, 1024)
	conn, err := net.Dial("udp", self.Master + ":" + self.PortUDP)
	if err != nil {
		fmt.Println(err)
                return
        }
	msg := "JOIN " + self.VmId
	join_buffer := []byte(msg)
        _, err = conn.Write(join_buffer)
        if err != nil {
        	fmt.Println(err)
		return
        }
	reqLen, err := conn.Read(buf)
	if err != nil {
		fmt.Println(err)
		return
	}	
	conn.Close()
	reqArr = strings.Split(string(buf[:reqLen]), " ")
	self.IsActive = true
	return reqArr, err
}

//This function print vm id
func (self *Daemon) PrintId() {
	fmt.Println(self.VmId)
}

