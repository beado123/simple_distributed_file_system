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
    "log"
    "os"
    "math/rand"
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
	Ln *Listener
	PortNum string
	MembershipList map[string]*Node
	IsActive bool
	//MW io.Writer
	Master string
	MyMutex *sync.Mutex
}

func NewDaemon(port string, id string) (d *Daemon, err error) {
	ip_address := getIPAddrAndLogfile()
	vm_id := ip_address[15:17]
	l, err := net.Listen("tcp", ip_address + ":" + port)
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
		PortNum: port,
		MembershipList: make(map[string]*Node),
		IsActive: true,
		//MW: mw,
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

func (self *Daemon) DaemonListen() {
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
		go ParseRequest(conn)
	}
}

func (self *Daemon) ParseRequest(conn net.Conn) {
	bufferRequest := make([]byte, 64)
	conn.Read(bufferRequest)
	request := string(bufferRequest)
	if request == "put_file" {
		self.ReceivePutRequest(conn)
	} else if request == "get_id" {
		self.ReceiveGetRequestAndSendFileVersion(conn)
	} else if request == "get_file" {
		self.ReceiveGetRequestAndSendFile(conn)
	}
}

func (self *Daemon) ReceivePutRequest(conn net.Conn) {
	//read file size and file name first
	bufferDirectoryName := make([]byte, 64)
	bufferFileSize := make([]byte, 10)
	l1, _ := conn.Read(bufferFileSize)
	fileSize, _ := strconv.ParseInt(string(bufferFileSize[:l1]), 10, 64)
	l2, _ := conn.Read(bufferDirectoryName)
	directoryName := string(bufferDirectoryName[:l2])
	
	
	//get file name
	num := GetFileId(directoryName)
	reqArr := strings.Split(directoryName, "/")
	fileName := directoryName + "/" + reqArr[1] + "_" + strconv.Itoa(num)

	//create new file
	newFile, err := os.Create(fileName)
	if err != nil {
		panic(err)
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

func (self *Daemon) SendPutRequest(cmd string) {
	//connect to master
	conn, err := net.Dial("tcp", self.Master + ":" + self.PortNum)
	if err != nil {
		fmt.Println(err)
		return
	}

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
	conn.Close()	

	//connect to each replica host
	var wg sync.WaitGroup
	var count := 0
	wg.Add(len(reqArr))
	for _, id := range reqArr {
		go func(id string, cmd string) {
			localFileName, sdfsFileName := ParsePutRequest(cmd)
			if id == self.VmId {
				//move local file to sdfs
				num := GetFileId(sdfsFileName)
				reqArr := strings.Split(sdfsFileName, "/")
        			fileName := sdfsFileName + "/" + reqArr[1] + "_" + strconv.Itoa(num)
				err := FileCopy(localFileName, fileName)
				if err == nil {
					count += 1
				}
				wg.Done()
				return
			}

			name := "fa18-cs425-g69-" + id + ".cs.illinois.edu"
			conn, err := net.Dial("tcp", name + ":" + self.PortNum)
			if err != nil {
                		fmt.Println(err)
				wg.Done()
                		return
        		}
			defer conn.Close()
			//read from localfile
			request := "put_file"
			file, err := os.Open(localFileName)
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
			conn.Write([]byte(request))
			conn.Write([]byte(fileSize))
			conn.Write([]byte(sdfsFileName))
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
		}(id, cmd)
	}	
	wg.Wait()
	
	//check if receive all putACK
	if count == len(reqArr) {
		fmt.Println("put successfully!")
	} else {
		fmt.Println("put fail!")
	}
}

func (self *Daemon) ReceiveGetRequestAndSendFileVersion(conn net.Conn) {
	defer conn.Close()
	//find file name
	bufferDirectoryName := make([]byte, 64)
	reqLen, _ := conn.Read(bufferDirectoryName)
	directoryName := string(bufferDirectoryName[:reqLen])
	num := GetFileId(directoryName)
	version := strconv.Itoa(num - 1)
	conn.Write([]byte(version))
}

func (self *Daemon) ReceiveGetRequestAndSendFile(conn net.Conn) {
	defer conn.Close()
	//find file name
	bufferDirectoryName := make([]byte, 64)
        reqLen, _ := conn.Read(bufferDirectoryName)
        directoryName := string(bufferDirectoryName[:reqLen])
	num := GetFileId(directoryName)
        reqArr := strings.Split(directoryName, "/")
        fileName := directoryName + "/" + reqArr[1] + "_" + strconv.Itoa(num - 1)

	//read file
	file, err := os.Open(fileName)
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

func (self *Daemon) SendGetRequest(cmd string) {
	//connect to master
        conn, err := net.Dial("tcp", self.Master + ":" + self.PortNum)
        if err != nil {
                fmt.Println(err)
                return
        }

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
        conn.Close()

	//connect to each replica host
	var wg sync.WaitGroup
      	latestVersion := 0
	vmId := 0
        wg.Add(len(reqArr))
        for _, id := range reqArr {
                go func(id string, cmd string) {
			localFileName, sdfsFileName := ParseGetRequest(cmd)
			if id == self.VmId {
				num := GetFileId(sdfsFileName)
				currVersion := num - 1
				if currVersion > latestVersion {
                                	latestVersion = currVersion
                                	vmId = id
	                        }
				wg.Done()
				return
			}

			name := "fa18-cs425-g69-" + id + ".cs.illinois.edu"
                        conn, err := net.Dial("tcp", name + ":" + self.PortNum)
                        if err != nil {
                                fmt.Println(err)
                                wg.Done()
                                return
                        }
                        defer conn.Close()			
                        request := "get_id"
			conn.Write([]byte(request))
                        conn.Write([]byte(sdfsFileName))

			bufferFileVersion := make([]byte, 64)	
			reqLen, _ := conn.Read(bufferFileVersion)
			currVersion := strconv.Atoi(string(bufferFileVersion[:reqLen]))
			if currVersion > latestVersion {
				latestVersion = currVersion
				vmId = id
			}
			wg.Done()
		}(id, cmd)
        }
        wg.Wait()	
	
	//connect the latest replica
	if self.VmId == vmId {
		num := GetFileId(sdfsFileName)
                reqArr = strings.Split(sdfsFileName, "/")
                fileName := sdfsFileName + "/" + reqArr[1] + "_" + strconv.Itoa(num)
               	FileCopy(fileName, localFileName)
		return 
	}
	name := "fa18-cs425-g69-" + vmId + ".cs.illinois.edu"
        conn, err := net.Dial("tcp", name + ":" + self.PortNum)
        if err != nil {
        	fmt.Println(err)
        	return
        }
        defer conn.Close()	
	localFileName, sdfsFileName := ParseGetRequest(cmd)
	request := "get_file"
	conn.Write([]byte(request))
	conn.Write([]byte(sdfsFileName))
	bufferFileSize := make([]byte, 10)
        reqLen, _ := conn.Read(bufferFileSize)
        fileSize, _ := strconv.ParseInt(string(bufferFileSize[:reqLen]), 10, 64)
	
	//create new file
	newFile, err := os.Create(localFileName)
        if err != nil {
                panic(err)
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
}

func (self *Daemon) ReceiveDeleteRequest(conn net.Conn) {
	defer conn.Close()
	bufferDirectoryName := make([]byte, 64)
	reqLen, _ := conn.Read(bufferDirectoryName)
	directoryName := string(bufferDirectoryName[:reqLen])
	err := DeleteSdfsfile(directoryName)
	if err != nil {
		fmt.Println(err)
		return
	}	
	response := "deleteACK"
	conn.Write([]byte(response))
}

func (self *Daemon) SendDeleteRequest(cmd string) {
	//connect to master
        conn, err := net.Dial("tcp", self.Master + ":" + self.PortNum)
        if err != nil {
                fmt.Println(err)
                return
        }

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
        conn.Close()

	var wg sync.WaitGroup
	var count := 0
        wg.Add(len(reqArr))
        for _, id := range reqArr {
                go func(id string, cmd string) {
			sdfsFileName := ParseDeleteRequest(cmd)
			if id == self.VmId {
				err := DeleteSdfsfile(sdfsFileName)
				if err == nil {
					count += 1
				}
				wg.Done()
				return
			}

			name := "fa18-cs425-g69-" + id + ".cs.illinois.edu"
                        conn, err := net.Dial("tcp", name + ":" + self.PortNum)
                        if err != nil {
                                fmt.Println(err)
                                wg.Done()
                                return
                        }
                        defer conn.Close()

			request := "delete_file"
			conn.Write([]byte(request))
                        conn.Write([]byte(sdfsFileName))
			
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
        conn, err := net.Dial("tcp", self.Master + ":" + self.PortNum)
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
	for _, id := range reqArr {
		name := "fa18-cs425-g69-" + id + ".cs.illinois.edu"
		fmt.Println(name)
	}
}

func (self *Daemon) StoreRequest() {
	files, err := ioutil.ReadDir("./sdfs")
    	if err != nil {
        	fmt.Println(err)
		return
    	}
    	for _, f := range files {
        	fmt.Println(f.Name())
    	}
}

func (self *Daemon) SendGetVersionRequest(cmd string) {
	//connect to master
        conn, err := net.Dial("tcp", self.Master + ":" + self.PortNum)
        if err != nil {
                fmt.Println(err)
                return
        }

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
        conn.Close()

	var wg sync.WaitGroup
        for _, id := range reqArr {
                go func(id string, cmd string) {
			if id == self.VmId {
							
			}
			
			wg.Done()		
		}(id, cmd)
        }
        wg.Wait()		
}
////////////////////helper function////////////////////////////////////////////////
func FileCopy(source string, destination string) error{
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

func ParsePutRequest(cmd string) (localFileName string, sdfsFileName string) {
	reqArr := strings.Split(cmd, " ")
        localFileName := reqArr[1]
        sdfsFileName := reqArr[2]
	return 
}

func ParseGetRequest(cmd string) (localFileName string, sdfsFileName string) {
        reqArr := strings.Split(cmd, " ")
        localFileName := reqArr[2]
        sdfsFileName := reqArr[1]
        return
}

func ParseDeleteRequest(cmd string) (sdfsFileName string) {
	reqArr := strings.Split(cmd, " ")
	sdfsFileName := reqArr[1]
	return
}

func ParseGetVersionRequest(cmd string) (localFileName string, sdfsFileName string, num string) {
        reqArr := strings.Split(cmd, " ")
        localFileName := reqArr[3]
        sdfsFileName := reqArr[1]
	num := reqArr[2]
        return
}

func GetFileId(directoryName string) int{
        //check if this directory exists
        if _, err := os.Stat(directoryName); os.IsNotExist(err) {
                os.Mkdir(directoryName, 0666)
        }
        files,_ := ioutil.ReadDir(directoryName)
        num := len(files) + 1
        return num
}

func DeleteSdfsfile(directoryName string) error{
	err := os.Remove(path)
	return err	
}


