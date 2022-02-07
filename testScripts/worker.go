package main

import (
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	zmq "github.com/pebbe/zmq4"
)

var (
	ip         string
	port       int
	nProcesses int
	verbose    bool
	xPrint     int
)

func init() {
	flag.StringVar(&ip, "i", "169.254.254.1", "ip of EIGER2 DCU")
	flag.IntVar(&port, "p", 9999, "zmq port")
	flag.IntVar(&nProcesses, "n", 4, "number of receiver processes")
	flag.BoolVar(&verbose, "v", false, "acknowledge every frame")
	flag.IntVar(&xPrint, "x", 100, "print acknowledgement every n-th frame")
	flag.Parse()
}

func work(ip string, port, id, xPrint int, wg *sync.WaitGroup) {
	framesReceived := 0
	log.Printf("worker %d printing after %d frames\n", id, xPrint)

	defer wg.Done()

	context, _ := zmq.NewContext()
	defer context.Term()

	socket, _ := context.NewSocket(zmq.PULL)
	defer socket.Close()

	host := fmt.Sprintf("tcp://%s:%d", ip, port)
	socket.Connect(host)

	poller := zmq.NewPoller()
	poller.Add(socket, zmq.POLLIN)

	log.Printf("worker %d polling from %s\n", id, host)

	for {
		polledSockets, _ := poller.Poll(time.Millisecond)
		for _, polled := range polledSockets {
			msg := receiveMultipart(polled.Socket)
			framesReceived++
			if verbose {
				log.Printf("worker %d received new frame with length %d\n", id, len(msg))
			}
			if framesReceived%xPrint == 0 {
				log.Printf("worker %d received total %d frames\n", id, framesReceived)
			}

		}
	}
}

func receiveMultipart(socket *zmq.Socket) [][]byte {

	multiPartMessage := make([][]byte, 9)
	index := 0
	multiPartMessage[index], _ = socket.RecvBytes(0)
	for more, _ := socket.GetRcvmore(); more; {
		index++
		multiPartMessage[index], _ = socket.RecvBytes(0)
		more, _ = socket.GetRcvmore()
	}
	return multiPartMessage[:index+1]
}

func main() {
	var wg sync.WaitGroup
	for i := 0; i < nProcesses; i++ {
		wg.Add(1)
		go work(ip, port, i, xPrint, &wg)
	}
	wg.Wait()
}
