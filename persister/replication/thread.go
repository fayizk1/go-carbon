package replication

import (
	"fmt"
	"log"
	"net"
	"time"
	"bytes"
	"bufio"
	"strconv"
	"strings"
	"encoding/json"
	"github.com/fayizk1/go-carbon/points"
)

type LevelReplicationThread struct {
	rlog *LevelReplicationLog
	PeerList []string
	server string
	out chan *points.Points
	AdminPasswordHash string
	startSlave bool
}

func NewReplicationThread(rlog *LevelReplicationLog, PeerList []string, server string, passwordHash string, out chan *points.Points) (*LevelReplicationThread) {
	return &LevelReplicationThread{rlog : rlog, PeerList: PeerList, out : out, server: server, AdminPasswordHash: passwordHash, startSlave: false}
}

func (rt *LevelReplicationThread) Start() {
	for i := range rt.PeerList {
		go rt.startReader(rt.PeerList[i])
	}
	go rt.startWriter()
}

func (rt *LevelReplicationThread) startReader(addr string) {
	pos, err := rt.rlog.GetReaderPos([]byte(addr))
	if err != nil {
		log.Println("Unable to read the thead postion", addr, pos)
		panic(err)
	}
	log.Printf("Starting read slave %s at %d", addr, pos)
	go func() { //Reader position
		log.Println("starting pos logger for ", addr)
		tick:= time.NewTicker(100 * time.Millisecond)
		for {
			select {
			case <-tick.C:
				err := rt.rlog.SetReaderPos([]byte(addr), pos)
				if err != nil {
					log.Println("Unable to write reader pos", err)
					break
				}
				log.Println("Written ",addr, " reader log at ", pos )
			}

		}
	}()
connect_expr:
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Println("Unable to connect server", err)
		time.Sleep(10 * time.Second) //wait before re-connect
		goto connect_expr
	}
	reader := bufio.NewReader(conn)
	if err := conn.SetReadDeadline(time.Now().Add(20 * time.Second)); err != nil {
		log.Println("Unable to set client read timout, restarting", err)
		 goto connect_expr
	}
	for {
		if rt.startSlave == false {
			time.Sleep(10 * time.Second)
			continue
		}
		queryCommand := fmt.Sprintf("GETLOG %d", pos)
		_, err := conn.Write([]byte(queryCommand + "\n"))
		if err != nil {
			log.Println("[replication] client write failed, ", err, ", reconnecting")
			conn.Close()
			goto connect_expr
		}
		if err := conn.SetReadDeadline(time.Now().Add(20 * time.Second)); err != nil {
			log.Println("Unable to set client read timout, restarting", err)
			goto connect_expr
		}
		message, err := reader.ReadBytes('\n')
		if err != nil {
			log.Println("[replication] client read failed, ", err, ", reconnecting")
			conn.Close()
			goto connect_expr
		}
		if bytes.HasPrefix( message,[]byte("ERRLAST")) {
			log.Println("No log to read, waiting 10 sec")
			time.Sleep(10 * time.Second)
			continue
		}
		if bytes.HasPrefix( message,[]byte("ERRTIMEOUT")) {
			log.Println("Log read timeout, waiting 10 sec")
			time.Sleep(10 * time.Second)
			continue
		}
		pos++
		messageSlice := bytes.Split(message, []byte("\x01"))
		if len(messageSlice) != 2 {
			log.Println("Droping Unknown message", string(message))
			continue
		}
		sPos, err := strconv.ParseUint(string(messageSlice[0]), 10, 64)
		if err != nil {
			log.Println("Droping Unknown message", string(message))
			continue
		}
		var pts *points.Points = &points.Points{}
		err = json.Unmarshal(bytes.TrimSpace(messageSlice[1]), pts)
		if err != nil {
			log.Println("Unable to parse packet, droping", string(message), err)
			continue
		}
		rt.out <- pts
		sPos++
		pos = sPos
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
				log.Println("TCP accept failed: %s", err)
			} else {
				log.Println("TCP Server Unknown error", err)
			}
			continue
		}
		go handleConnection(rt, conn)
	}
}

func handleConnection(rt *LevelReplicationThread, conn net.Conn) {
	raddr := conn.RemoteAddr().String()
	defer func() {
		log.Println("Closing connection...", raddr)
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
			log.Println(err)
			return
		}
		pktSlice := strings.Split(pkt, " ")
		switch strings.TrimSpace(pktSlice[0]) {
		case "GETLOG":
			if len(pktSlice) != 2 {
				_, err := conn.Write(append([]byte("ERRCONN"), '\x01', '_', '\n'))
				if err != nil {
					log.Println("Unable send packet to client, closing", err)
					return
				}
			}
			rPos, err := strconv.ParseUint(strings.TrimSpace(pktSlice[1]), 10, 64)
			if err != nil {
				_, err = conn.Write(append([]byte("ERRUNKNWNQRY"), '\x01', '_', '\n'))
				log.Println("Unable send packet to client, closining", err)
				continue mainloop
			}
			cPos := rt.rlog.Counter
			if rPos > cPos {
				_, err := conn.Write(append([]byte("ERRLAST"), '\x01', '_', '\n'))
				if err != nil {
					log.Println("Unable send packet to client, closining", err)
					return
				}
			}
			pos, val, err :=  rt.rlog.GetLogFirstAvailable(rPos)
			if err == ErrLogReadTimeout {
				_, err := conn.Write(append([]byte("ERRTIMEOUT"), '\x01', '_', '\n'))
				if err != nil {
					log.Println("Unable send packet to client, closining", err)
					return
				}
			} else if err != nil {
				_, err := conn.Write(append([]byte("ERRREAD"), '\x01', '_', '\n'))
				if err != nil {
					log.Println("Unable send packet to client, closining", err)
					return
				}
			}
			data := append([]byte(strconv.FormatUint(pos, 10)), '\x01')
			data = append(data, val...)
			data = append(data, '\n')
			_, err = conn.Write(data)
			if err != nil {
				log.Println("Unable send packet to client, closining", err)
				return
			}
		case "QUIT":
			log.Println("Closing connection", raddr)
			return
		case "STARTADMIN":
			log.Println("Got admin request from", raddr)
			admin = true
			_, err := conn.Write([]byte("started admin mode \n"))
			if err != nil {
				log.Println("Unable to write into admin mode, closing", err)
				return
			}			
		case "QUITADMIN":
			admin = false
			_, err := conn.Write([]byte("exited from admin mode \n"))
			if err != nil {
				log.Println("Unable to write into admin mode, closing", err)
				return
			}						
		case "STARTSLAVE":
			if !admin  {
				_, err := conn.Write([]byte("please login as admin mode \n"))
				if err != nil {
					log.Println("Unable to write into admin mode, closing", err)
					return
				}						
				continue mainloop
			}
			rt.startSlave = true
			_, err := conn.Write([]byte("started slave \n"))
			if err != nil {
				log.Println("Unable to write , closing", err)
				return
			}
		case "STOPSLAVE":
			if !admin  {
				_, err := conn.Write([]byte("please login as admin mode \n"))
				if err != nil {
					log.Println("Unable to write into admin mode, closing", err)
					return
				}
				continue mainloop
			}
			rt.startSlave = false
			_, err := conn.Write([]byte("stopped slave \n"))
			if err != nil {
				log.Println("Unable to write , closing", err)
				return
			}			
		case "SHOWSLAVES":
			plist := strings.Join(rt.PeerList, " ")
			statusText := "stopped"
			if rt.startSlave {
				statusText = "started"
			}
			data := "peer list: " + plist + "| state: " + statusText
			_, err := conn.Write([]byte(data))
			if err != nil {
				log.Println("Unable send packet to client, closining", err)
				return
			}
		case "READERSTATUS":
			if len(pktSlice) < 2 {
				_, err := conn.Write([]byte("Not enough args \n"))
				if err != nil {
					log.Println("Unable to write into admin mode, closing", err)
					return
				}
				continue mainloop
			}
			readerPos, err := rt.rlog.GetReaderPos([]byte(strings.TrimSpace(pktSlice[1])))
			if err != nil {
				log.Println("Error while read", err)
				_, err := conn.Write([]byte("Unable to read pos \n"))
				if err != nil {
					log.Println("Unable to write into admin mode, closing", err)
					return
				}
				continue mainloop
			}
			_, err = conn.Write([]byte(fmt.Sprintf("Slave Position: %d \n", readerPos)))
			if err != nil {
				log.Println("Unable to write into admin mode, closing", err)
				return
			}

		default:
			_, err := conn.Write(append([]byte("ERRUNKN"), '\x01', '_', '\n'))
			if err != nil {
				log.Println("Unable send packet to client, closining", err)
				return
			}
		}
	}
}

