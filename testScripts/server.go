package main

import (
	"flag"
	"fmt"
	"log"
	"sync"

	zmq "github.com/pebbe/zmq4"
)

var (
	ip        string
	port      int
	nMessages int
)

func init() {
	flag.StringVar(&ip, "i", "127.0.0.1", "ip of EIGER2 DCU")
	flag.IntVar(&port, "p", 9999, "zmq port")
	flag.IntVar(&nMessages, "n", 1000, "number of messages to send")
	flag.Parse()

}

func serve(ip string, port, nMessages int, wg *sync.WaitGroup) {
	defer wg.Done()

	context, _ := zmq.NewContext()
	defer context.Term()

	socket, _ := context.NewSocket(zmq.PUSH)
	defer socket.Close()

	host := fmt.Sprintf("tcp://%s:%d", ip, port)
	socket.Bind(host)

	log.Printf("serving %s\n", host)

	msg := newMultiPartMessage(0)
	for msgID := 0; msgID < nMessages; msgID++ {
		msg.messageNumber = msgID
		socket.SendMessage(msg)
		log.Printf("server sent message %d\n", msgID)
	}
}

type multiPartMessage struct {
	serverID      int
	messageNumber int
	blob          []byte
}

func newMultiPartMessage(id int) *multiPartMessage {
	m := multiPartMessage{serverID: id,
		messageNumber: 0}
	m.blob = make([]byte, 1024*1024*10)
	return &m
}

func main() {
	var wg sync.WaitGroup
	wg.Add(1)
	go serve(ip, port, nMessages, &wg)
	wg.Wait()
}
