# Simple Distributed File System

 ## To Start Master
 ```
 go run server.go
 ```
 ## To Start other machines
 ```
 go build client.go
 ./client id_of_master
 ```
 ## After runing server/client
 ## To print the membership list
 ```
 LIST
 ```
 ## To print self id
 ```
 SELF
 ```
 ## To join the group
 ```
 JOIN
 ```
 ## To leave the group
 ```
 LEAVE
 ```
 ## To fail current machine
 ```
 CTRL+C
 ```
  ## Operations in MP3
  # On Any machine other than master
  ## insert/update files
  ```
  put localfilename sdfsfilename
  ```
  ## download file to local directory
  ```
  get sdfsfilename localfilename
  ```
  ## delete file in SDFS and all its replicas
  ```
  delete sdfsfilename
  ```
  ## list all machine (VM) addresses where this file is currently being stored
  ```
  ls sdfsfilename
  ```
  ## gets all the last num-versions versions of the file into the localfilename
  ```
  get-versions sdfsfilename numversions localfilename
  ```
  ## list all files in local directory
  ```
  store
  ```
