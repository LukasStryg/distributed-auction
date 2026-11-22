package main

import (
	proto "auction/grpc"
	"bufio"
	"context"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// the client knows every node, but starts with the one it is given
var addresses = []string{"localhost:5000", "localhost:5001", "localhost:5002"}

const timeout = 10 * time.Second

var nodes []proto.AuctionClient
var current int

// on error, move on to the next node
func next(name string, err error) {
	log.Printf("client: %s got no answer from node %d: %v", name, current, err)
	current = (current + 1) % len(nodes)
}

func bid(name string, amount int64, seq int64) proto.Outcome {
	req := &proto.BidRequest{Bidder: name, Amount: amount, Seq: seq}
	for range nodes {
		log.Printf("client: %s bids %d (seq %d) at node %d", name, amount, seq, current)
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		reply, err := nodes[current].Bid(ctx, req)
		cancel()
		if err == nil {
			return reply.Outcome
		}
		next(name, err)
	}
	return proto.Outcome_EXCEPTION
}

func result(name string) {
	for range nodes {
		log.Printf("client: %s asks node %d for the result", name, current)
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		reply, err := nodes[current].Result(ctx, &proto.Empty{})
		cancel()
		if err != nil {
			next(name, err)
			continue
		}
		if reply.Winner == "" {
			log.Printf("client: %s got result: no bids yet (over %t)", name, reply.Over)
		} else if reply.Over {
			log.Printf("client: %s got result: auction is over, %s won with %d", name, reply.Winner, reply.Amount)
		} else {
			log.Printf("client: %s got result: highest bid is %d by %s", name, reply.Amount, reply.Winner)
		}
		return
	}
	log.Printf("client: %s got result: EXCEPTION", name)
}

func main() {
	if len(os.Args) < 3 {
		log.Fatalf("usage: go run ./client <name> <node id>")
	}
	name := os.Args[1]
	id, err := strconv.Atoi(os.Args[2])
	if err != nil || id < 0 || id >= len(addresses) {
		log.Fatalf("node id must be between 0 and %d", len(addresses)-1)
	}
	current = id

	// log to the terminal and to <name>.log
	file, err := os.Create(name + ".log")
	if err != nil {
		log.Fatalf("client: failed to create log file: %v", err)
	}
	defer file.Close()
	log.SetOutput(io.MultiWriter(os.Stdout, file))
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	for _, addr := range addresses {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("client: failed to connect to %s: %v", addr, err)
		}
		defer conn.Close()
		nodes = append(nodes, proto.NewAuctionClient(conn))
	}

	// unique id of a bid is (name, seq), the next bid is sent after the reply
	var seq int64
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 2 && fields[0] == "bid" {
			amount, err := strconv.ParseInt(fields[1], 10, 64)
			if err != nil {
				log.Println("client: amount must be a number")
				continue
			}
			seq++
			outcome := bid(name, amount, seq)
			log.Printf("client: %s got %s for bid %d", name, outcome, amount)
		} else if len(fields) == 1 && fields[0] == "result" {
			result(name)
		} else {
			log.Println("client: type \"bid <amount>\" or \"result\"")
		}
	}
}
