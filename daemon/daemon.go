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
	if request == "put" {
		self.ReceivePutRequest(conn)
	}
}

func getFileId(directoryName string) int{
	//check if this directory exists
        if _, err := os.Stat(directoryName); os.IsNotExist(err) {
                os.Mkdir(directoryName, 0666)
        }
        files,_ := ioutil.ReadDir(directoryName)
        num := len(files) + 1
	return num
}

func (self *Daemon) ReceivePutRequest(conn net.Conn) {
	//read file size and file name first
	bufferDirectoryName := make([]byte, 64)
	bufferFileSize := make([]byte, 10)
	l1, _ := conn.Read(bufferFileSize)
	fileSize, _ := strconv.ParseInt(string(bufferFileSize[:l1]), 10, 64)
	l2, _ := conn.Read(bufferDirectoryName[:l2])
	directoryName := string(bufferDirectoryName)
	
	
	//get file name
	num := getFileId(directoryName)
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
			io.CopyN(newFile, connection, (fileSize - receivedBytes))
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
			if id == vmId {
				//move local file to sdfs
				reqArr := strings.Split(cmd, " ")
                        	localFileName := reqArr[1]
                        	sdfsFileName := reqArr[2]
				num := getFileId(sdfsFileName)
				reqArr = strings.Split(sdfsFileName, "/")
        			fileName := sdfsFileName + "/" + reqArr[1] + "_" + strconv.Itoa(num)
				from, _ := os.Open(localFileName)
  				defer from.Close()
  				to, _ := os.Create(fileName)
  				defer to.Close()
  				_, _ = io.Copy(to, from)
				count += 1
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
			reqArr := strings.Split(cmd, " ")
			request := reqArr[0]
                        localFileName := reqArr[1]
                        sdfsFileName := reqArr[2]
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

func (self *Daemon) ReceiveGetRequestAndSendFileName(conn net.Conn) {
	
}

func (self *Daemon) ReceiveGetRequestAndSendFile(conn net.Conn) {
	defer conn.Close()
	bufferDirectoryName := make([]byte, 64)
        bufferFileSize := make([]byte, 10)
        l1, _ := conn.Read(bufferFileSize)
        fileSize, _ := strconv.ParseInt(string(bufferFileSize[:l1]), 10, 64)
        l2, _ := conn.Read(bufferDirectoryName[:l2])
        directoryName := string(bufferDirectoryName)
	num := getFileId(directoryName)
        reqArr := strings.Split(directoryName, "/")
        fileName := directoryName + "/" + reqArr[1] + "_" + strconv.Itoa(num - 1)

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

	var wg sync.WaitGroup
        var count := 0
        wg.Add(len(reqArr))
        for _, id := range reqArr {
                go func(id string, cmd string) {
			
		}(id, cmd)
        }
        wg.Wait()	
}
