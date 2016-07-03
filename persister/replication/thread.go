package replication

import (
	"fmt"
	"net"
	"time"
	"bytes"
	"bufio"
	"strconv"
	"sync"
	"strings"
	"encoding/json"
	"github.com/Sirupsen/logrus"
	"github.com/fayizk1/go-carbon/points"
)

type ReaderMeta struct {
	position uint64
	running bool
	sync.RWMutex
}

type LevelReplicationThread struct {
	rlog *LevelReplicationLog
	PeerList []string
	server string
	out chan *points.Points
	AdminPasswordHash string
	Readers map[string]*ReaderMeta
}

func NewReplicationThread(rlog *LevelReplicationLog, PeerList []string, server string, passwordHash string, out chan *points.Points) (*LevelReplicationThread) {
	return &LevelReplicationThread{rlog : rlog, PeerList: PeerList, out : out, server: server, AdminPasswordHash: passwordHash, Readers : make(map[string]*ReaderMeta)}
}

func (rt *LevelReplicationThread) Start() {
	for i := range rt.PeerList {
		rt.Readers[rt.PeerList[i]] = &ReaderMeta{running: false, position: 0}
		go rt.startReader(rt.PeerList[i])
	}
	go rt.startWriter()
}

func (rt *LevelReplicationThread) startReader(addr string) {
	pos, err := rt.rlog.GetReaderPos([]byte(addr))
	if err != nil {
		logrus.Println("[Replication Thread] Unable to read the thead postion", addr, pos)
		panic(err)
	}
	logrus.Printf("[Replication thread] Starting read slave %s at %d", addr, pos)
	rt.Readers[addr].position = pos
	go func() { //Reader position
		logrus.Println("[Replication thread] starting pos logger for ", addr)
		tick:= time.NewTicker(100 * time.Millisecond)
		for {
			select {
			case <-tick.C:
				rt.Readers[addr].RLock()
				position := rt.Readers[addr].position
				rt.Readers[addr].RUnlock()
				err := rt.rlog.SetReaderPos([]byte(addr), position)
				if err != nil {
					logrus.Println("[Replication thread] Unable to write reader pos", err)
					break
				}
			}

		}
	}()
connect_expr:
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		logrus.Println("[Replication Thread]Unable to connect server", err)
		time.Sleep(10 * time.Second) //wait before re-connect
		goto connect_expr
	}
	reader := bufio.NewReader(conn)
	if err := conn.SetReadDeadline(time.Now().Add(20 * time.Second)); err != nil {
		logrus.Println("[replication Thread] Unable to set client read timout, restarting", err)
		 goto connect_expr
	}
	var running bool
	var position uint64
	for {
		rt.Readers[addr].RLock()
		running = rt.Readers[addr].running
		rt.Readers[addr].RUnlock()
		if !running {
			time.Sleep(10 * time.Second)
			continue
		}
		rt.Readers[addr].RLock()
		position = rt.Readers[addr].position
		rt.Readers[addr].RUnlock()
		queryCommand := fmt.Sprintf("GETLOG %d", position)
		_, err := conn.Write([]byte(queryCommand + "\n"))
		if err != nil {
			logrus.Println("[replication Thread] client write failed, ", err, ", reconnecting")
			conn.Close()
			goto connect_expr
		}
		if err := conn.SetReadDeadline(time.Now().Add(20 * time.Second)); err != nil {
			logrus.Println("[Replication Thread] Unable to set client read timout, restarting", err)
			goto connect_expr
		}
		message, err := reader.ReadBytes('\n')
		if err != nil {
			logrus.Println("[Replication Thread] client read failed, ", err, ", reconnecting")
			conn.Close()
			goto connect_expr
		}
		if bytes.HasPrefix( message,[]byte("ERRLAST")) {
			logrus.Println("[Replication Thread] No log to read, waiting 10 sec")
			time.Sleep(10 * time.Second)
			continue
		}
		messageSlice := bytes.Split(message, []byte("\x01"))
		if len(messageSlice) != 2 {
			logrus.Println("[Replication Thread] Unknown message", string(message), addr)
			time.Sleep(1 * time.Second)
			continue
		}
		sPos, err := strconv.ParseUint(string(messageSlice[0]), 10, 64)
		if err != nil {
			logrus.Println("[Replication Thread] Unknown message", string(message), addr)
			time.Sleep(1 * time.Second)
			continue
		}
		if sPos != position {
			logrus.Printf("[Replication Thread] Unknown log postion recieved, old : %d, recieved : %d, server: %s", pos, sPos, addr)
			time.Sleep(1 * time.Second)
			continue
		}
		var pts *points.Points = &points.Points{}
		err = json.Unmarshal(bytes.TrimSpace(messageSlice[1]), pts)
		if err != nil {
			logrus.Println("[Replication Thread] Unable to parse packet", string(message), err, addr)
			time.Sleep(1 * time.Second)
			continue
		}
		rt.out <- pts
		rt.Readers[addr].Lock()
		rt.Readers[addr].position++
		rt.Readers[addr].Unlock()
	}
}

func (rt *LevelReplicationThread) startWriter() {
	address, err := net.ResolveTCPAddr("tcp", rt.server)
	if err != nil {
		panic(err)
	}
	listener, err := net.ListenTCP("tcp", address)
	if err != nil {
		panic(err)
	}
	for {
		conn, err := listener.Accept()
		if  err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Temporary() {
				logrus.Println("[Replication Thread] TCP accept failed: %s", err)
			} else {
				logrus.Println("[Replication Thread] TCP Server Unknown error", err)
			}
			continue
		}
		go handleConnection(rt, conn)
	}
}

func handleConnection(rt *LevelReplicationThread, conn net.Conn) {
	raddr := conn.RemoteAddr().String()
	defer func() {
		logrus.Println("[Replication Thread] Closing connection...", raddr)
		conn.Close()
	}()
	bufReader := bufio.NewReader(conn)
	admin := false
	_ = admin
mainloop:
	for {
		conn.SetReadDeadline(time.Now().Add(20 * time.Second))
		pkt, err := bufReader.ReadString('\n')
		if err != nil {
			logrus.Println("[Replication thread]", err)
			return
		}
		pktSlice := strings.Split(pkt, " ")
		switch strings.TrimSpace(pktSlice[0]) {
		case "GETLOG":
			if len(pktSlice) != 2 {
				_, err := conn.Write(append([]byte("ERRCOMM"), '\x01', '_', '\n'))
				if err != nil {
					logrus.Println("[Replication Thread] Unable send packet to client, closing", err)
					return
				}
				continue mainloop
			}
			rPos, err := strconv.ParseUint(strings.TrimSpace(pktSlice[1]), 10, 64)
			if err != nil {
				_, err = conn.Write(append([]byte("ERRUNKNWNQRY"), '\x01', '_', '\n'))
				if err != nil {
					logrus.Println("[Replication Thread] Unable send packet to client, closing", err)
					return
				}
				continue mainloop
			}
			cPos := rt.rlog.Counter
			if rPos > cPos {
				_, err := conn.Write(append([]byte("ERRLAST"), '\x01', '_', '\n'))
				if err != nil {
					logrus.Println(" [Replication Thread] Unable send packet to client, closining", err)
					return
				}
				continue mainloop
			}
			val, err :=  rt.rlog.GetLog(rPos)
			if err != nil {
				errMsg := append([]byte("ERRREAD"), '\x01', '_')
				errMsg = append(errMsg, []byte(err.Error())...)
				errMsg = append(errMsg, '\n')
				_, err := conn.Write(errMsg)
				if err != nil {
					logrus.Println("[Replication Thread] Unable send packet to client, closining", err)
					return
				}
				continue mainloop
			}
			data := append([]byte(strconv.FormatUint(rPos, 10)), '\x01')
			data = append(data, val...)
			data = append(data, '\n')
			_, err = conn.Write(data)
			if err != nil {
				logrus.Println("[Replication Thread] Unable send packet to client, closining", err)
				return
			}
		case "QUIT":
			logrus.Println("[Replication Thread] Closing connection", raddr)
			return
		case "STARTADMIN":
			logrus.Println("[Replication Thread] Got admin request from", raddr)
			admin = true
			_, err := conn.Write([]byte("started admin mode \n"))
			if err != nil {
				logrus.Println("[Replication Thread] Unable to write into admin mode, closing", err)
				return
			}			
		case "QUITADMIN":
			admin = false
			_, err := conn.Write([]byte("exited from admin mode \n"))
			if err != nil {
				logrus.Println("[Replication Thread] Unable to write into admin mode, closing", err)
				return
			}						
		case "STARTREADER":
			if !admin  {
				_, err := conn.Write([]byte("please login as admin mode \n"))
				if err != nil {
					logrus.Println("[Replication Thread] Unable to write into admin mode, closing", err)
					return
				}						
				continue mainloop
			}
			if len(pktSlice) < 2 {
				_, err := conn.Write([]byte("Not enough input \n"))
				if err != nil {
					logrus.Println("[Replication Thread] Unable to write into admin mode, closing", err)
					return
				}
				continue mainloop
			}
			readerName := strings.TrimSpace(pktSlice[1])
			if strings.ToLower(readerName) == "all" {
				for _, v := range rt.Readers {
					v.Lock()
					v.running = true
					v.Unlock()
				}
			} else {
				if v, ok := rt.Readers[readerName]; ok {
					v.Lock()
					v.running = true
					v.Unlock()
				} else {
					_, err := conn.Write([]byte("Unknow slave \n"))
					if err != nil {
						logrus.Println("[Replication Thread] Unable to write , closing", err)
						return
					}
					continue mainloop
				}
			}
			_, err := conn.Write([]byte("started slave \n"))
			if err != nil {
				logrus.Println("[Replication Thread] Unable to write , closing", err)
				return
			}
		case "STOPREADER":
			if !admin  {
				_, err := conn.Write([]byte("please login as admin mode \n"))
				if err != nil {
					logrus.Println("[Replication Thread] Unable to write into admin mode, closing", err)
					return
				}
				continue mainloop
			}
			if len(pktSlice) < 2 {
				_, err := conn.Write([]byte("Not enough input \n"))
				if err != nil {
					logrus.Println("[Replication Thread] Unable to write into admin mode, closing", err)
					return
				}
				continue mainloop
			}
			readerName := strings.TrimSpace(pktSlice[1])
			if strings.ToLower(readerName) == "all" {
				for _, v := range rt.Readers {
					v.Lock()
					v.running = false
					v.Unlock()
				}
			} else {
				if v, ok := rt.Readers[readerName]; ok {
					v.Lock()
					v.running = false
					v.Unlock()
				} else {
					_, err := conn.Write([]byte("Unknown slave \n"))
					if err != nil {
						logrus.Println("[Replication Thread] Unable to write , closing", err)
						return
					}
					continue mainloop
				}
			}
			_, err := conn.Write([]byte("stopped slave \n"))
			if err != nil {
				logrus.Println("[Replication Thread] Unable to write , closing", err)
				return
			}
		case "SHOWWRITER":
			statusMsg := "------------------------------------\n"
			statusMsg += fmt.Sprintf("position: %d\n" ,  rt.rlog.Counter)
 			_, err := conn.Write([]byte(statusMsg))
			if err != nil {
				logrus.Println("[Replication Thread] Unable send packet to client, closining", err)
				return
			}
		case "SHOWREADERS":
			var statusMsg string
			for k, v := range rt.Readers {
				v.RLock()
				statusMsg += "------------------------------------\n"
				statusMsg += "peer: " + k + "\n"
				statusMsg += "postion: " + strconv.FormatUint(v.position, 10) + "\n"
				statusMsg += "running" + strconv.FormatBool(v.running) + "\n"
				v.RUnlock()
			}
 			_, err := conn.Write([]byte(statusMsg))
			if err != nil {
				logrus.Println("[Replication Thread] Unable send packet to client, closining", err)
				return
			}
		case "READERSTATUS":
			if len(pktSlice) < 2 {
				_, err := conn.Write([]byte("Not enough args \n"))
				if err != nil {
					logrus.Println("[Replication Thread] Unable to write into admin mode, closing", err)
					return
				}
				continue mainloop
			}
			if v, ok := rt.Readers[strings.TrimSpace(pktSlice[1])]; ok {
				v.RLock()
				statusMsg := "------------------------------------\n"
				statusMsg += "peer: " + strings.TrimSpace(pktSlice[1]) + "\n"
				statusMsg += "postion: " + strconv.FormatUint(v.position, 10) + "\n"
				statusMsg += "running" + strconv.FormatBool(v.running) + "\n"
				v.RUnlock()
				_, err := conn.Write([]byte(statusMsg))
				if err != nil {
					logrus.Println("[Replication Thread] Unable send packet to client, closining", err)
					return
				}
			} else {
				_, err := conn.Write([]byte("Unknown peer , please use SHOWREADERS\n"))
				if err != nil {
					logrus.Println("[Replication Thread] Unable send packet to client, closining", err)
					return
				}
			}
		case "SETREADER":
			if !admin  {
				_, err := conn.Write([]byte("please login as admin mode \n"))
				if err != nil {
					logrus.Println("[Replication Thread] Unable to write into admin mode, closing", err)
					return
				}						
				continue mainloop
			}
			if len(pktSlice) < 3 {
				_, err := conn.Write([]byte("Not enough args \n"))
				if err != nil {
					logrus.Println("[Replication Thread] Unable to write into admin mode, closing", err)
					return
				}
				continue mainloop
			}
			if v, ok := rt.Readers[strings.TrimSpace(pktSlice[1])]; ok {
				var statusMsg string
				v.Lock()
				if v.running {
					statusMsg = "Please stop slave before setting position\n"
				} else {
					positionTxt :=  strings.TrimSpace(pktSlice[2])
					if strings.HasPrefix(positionTxt, "+") || strings.HasPrefix(positionTxt, "=") || strings.HasPrefix(positionTxt, "-") {
						sp, err := strconv.ParseUint(positionTxt[1:], 10, 64)
						if err != nil {
							statusMsg = "Unable to parse the positin text " + err.Error() + "\n"
						} else {
							if positionTxt[0] == '+' {
								v.position += sp
							} else if positionTxt[0] == '-' {
								v.position -= sp
							} else if positionTxt[0] == '=' {
								v.position = sp
							}
							statusMsg = "position set to " + strconv.FormatUint(v.position, 10) + ", reader: "+ pktSlice[1] + "\n"
						}
					}
				}
				v.Unlock()
				_, err := conn.Write([]byte(statusMsg))
				if err != nil {
					logrus.Println("[Replication Thread] Unable send packet to client, closining", err)
					return
				}
			} else {
				_, err := conn.Write([]byte("Unknown peer , please use SHOWREADERS\n"))
				if err != nil {
					logrus.Println("[Replication Thread] Unable send packet to client, closining", err)
					return
				}
			}

		default:
			_, err := conn.Write(append([]byte("ERRUNKN"), '\x01', '_', '\n'))
			if err != nil {
				logrus.Println("[Replication Thread] Unable send packet to client, closining", err)
				return
			}
			
		}
	}
}
