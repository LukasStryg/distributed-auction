package main

import (
	proto "auction/grpc"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// every node knows the address of every other node
var addresses = []string{"localhost:5000", "localhost:5001", "localhost:5002"}

// the auction ends 100 seconds after it starts
const duration = 100 * time.Second

// no answer within timeout means the node has crashed
const timeout = time.Second

type node struct {
	proto.UnimplementedAuctionServer
	id      int32
	peers   []proto.AuctionClient // indexed by id, nil for ourselves
	mu      sync.Mutex
	primary int32
	state   *proto.State
	dead    []bool     // nodes we know have crashed
	elect   sync.Mutex // one election at a time
}

func (n *node) getPrimary() int32 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.primary
}

func over(deadline int64) bool {
	return time.Now().UnixMilli() >= deadline
}

// any node: handle the bid if we are the primary, else forward it to the primary
func (n *node) Bid(ctx context.Context, in *proto.BidRequest) (*proto.Ack, error) {
	// at most one crash, so at most one new primary
	for range 2 {
		p := n.getPrimary()
		if p == n.id {
			return n.bid(in), nil
		}
		log.Printf("node %d: forwarding bid %d from %s (seq %d) to primary %d", n.id, in.Amount, in.Bidder, in.Seq, p)
		ctx, cancel := context.WithTimeout(context.Background(), 5*timeout)
		reply, err := n.peers[p].Bid(ctx, in)
		cancel()
		if err == nil {
			return reply, nil
		}
		log.Printf("node %d: primary %d did not answer: %v", n.id, p, err)
		n.election(p)
	}
	return &proto.Ack{Outcome: proto.Outcome_EXCEPTION}, nil
}

// any node: same as Bid
func (n *node) Result(ctx context.Context, in *proto.Empty) (*proto.ResultReply, error) {
	for range 2 {
		p := n.getPrimary()
		if p == n.id {
			return n.result(), nil
		}
		log.Printf("node %d: forwarding result to primary %d", n.id, p)
		ctx, cancel := context.WithTimeout(context.Background(), 5*timeout)
		reply, err := n.peers[p].Result(ctx, in)
		cancel()
		if err == nil {
			return reply, nil
		}
		log.Printf("node %d: primary %d did not answer: %v", n.id, p, err)
		n.election(p)
	}
	return nil, fmt.Errorf("no primary")
}

// primary: one request at a time
func (n *node) bid(in *proto.BidRequest) *proto.Ack {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state.Bidders == nil {
		n.state.Bidders = make(map[string]*proto.Bidder)
	}
	b, ok := n.state.Bidders[in.Bidder]
	// already executed, resend the response
	if ok && b.Seq == in.Seq {
		log.Printf("node %d: bid %d from %s (seq %d) already done, resending %s", n.id, in.Amount, in.Bidder, in.Seq, b.Outcome)
		return &proto.Ack{Outcome: b.Outcome}
	}
	if !ok {
		log.Printf("node %d: registered bidder %s", n.id, in.Bidder)
	}

	outcome := proto.Outcome_SUCCESS
	if over(n.state.Deadline) {
		outcome = proto.Outcome_FAIL
		log.Printf("node %d: bid %d from %s failed, auction is over", n.id, in.Amount, in.Bidder)
	} else if in.Amount <= n.state.Highest {
		outcome = proto.Outcome_FAIL
		log.Printf("node %d: bid %d from %s failed, highest bid is %d", n.id, in.Amount, in.Bidder, n.state.Highest)
	} else {
		n.state.Highest = in.Amount
		n.state.Winner = in.Bidder
		log.Printf("node %d: bid %d from %s is the highest bid", n.id, in.Amount, in.Bidder)
	}
	n.state.Bidders[in.Bidder] = &proto.Bidder{Seq: in.Seq, Outcome: outcome}

	// wait for the backups before responding
	n.replicate(false)
	return &proto.Ack{Outcome: outcome}
}

// primary
func (n *node) result() *proto.ResultReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	return &proto.ResultReply{Over: over(n.state.Deadline), Winner: n.state.Winner, Amount: n.state.Highest}
}

// send the state to every live backup and wait for the acks, caller holds mu
func (n *node) replicate(coordinator bool) {
	var wg sync.WaitGroup
	for i, peer := range n.peers {
		if peer == nil || n.dead[i] {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			var err error
			if coordinator {
				_, err = peer.Coordinator(ctx, n.state)
			} else {
				_, err = peer.Update(ctx, n.state)
			}
			if err != nil {
				log.Printf("node %d: no ack from node %d, it has crashed", n.id, i)
				n.dead[i] = true
				return
			}
			log.Printf("node %d: ack from node %d", n.id, i)
		}()
	}
	wg.Wait()
}

// backup: on receive update from the primary
func (n *node) Update(ctx context.Context, in *proto.State) (*proto.Empty, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.state = in
	log.Printf("node %d: got update from primary %d (highest %d by %s)", n.id, in.Primary, in.Highest, in.Winner)
	return &proto.Empty{}, nil
}

// bully: on receive election, send answer and call our own election
func (n *node) Election(ctx context.Context, in *proto.ElectionRequest) (*proto.Empty, error) {
	log.Printf("node %d: got election from node %d, sending answer", n.id, in.Id)
	go n.election(n.getPrimary())
	return &proto.Empty{}, nil
}

// bully: on receive coordinator(m), m is the new primary
func (n *node) Coordinator(ctx context.Context, in *proto.State) (*proto.Empty, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.primary = in.Primary
	n.state = in
	log.Printf("node %d: got coordinator from node %d, it is the primary (highest %d by %s)", n.id, in.Primary, in.Highest, in.Winner)
	return &proto.Empty{}, nil
}

// bully: called when primary dead does not answer
func (n *node) election(dead int32) {
	n.elect.Lock()
	defer n.elect.Unlock()

	for n.getPrimary() == dead {
		log.Printf("node %d: calling election", n.id)
		answered := false
		for i := n.id + 1; i < int32(len(addresses)); i++ {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			_, err := n.peers[i].Election(ctx, &proto.ElectionRequest{Id: n.id})
			cancel()
			if err == nil {
				log.Printf("node %d: got answer from node %d", n.id, i)
				answered = true
			}
		}

		// timeout without answer, all higher ids are gone
		if !answered {
			n.mu.Lock()
			n.primary = n.id
			n.state.Primary = n.id
			log.Printf("node %d: no answer, sending coordinator to all", n.id)
			// coordinator(m) with our state
			n.replicate(true)
			n.mu.Unlock()
			return
		}

		// wait for coordinator, on timeout call a new election
		end := time.Now().Add(3 * timeout)
		for n.getPrimary() == dead && time.Now().Before(end) {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("usage: go run ./node <id>")
	}
	id, err := strconv.Atoi(os.Args[1])
	if err != nil || id < 0 || id >= len(addresses) {
		log.Fatalf("id must be between 0 and %d", len(addresses)-1)
	}

	// log to the terminal and to node<id>.log
	file, err := os.Create(fmt.Sprintf("node%d.log", id))
	if err != nil {
		log.Fatalf("node %d: failed to create log file: %v", id, err)
	}
	defer file.Close()
	log.SetOutput(io.MultiWriter(os.Stdout, file))
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// the highest id starts as primary
	n := &node{
		id:      int32(id),
		primary: int32(len(addresses) - 1),
		state:   &proto.State{Primary: int32(len(addresses) - 1)},
		peers:   make([]proto.AuctionClient, len(addresses)),
		dead:    make([]bool, len(addresses)),
	}

	// client part, one connection to every other node
	for i, addr := range addresses {
		if i == id {
			continue
		}
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("node %d: failed to connect to %s: %v", id, addr, err)
		}
		defer conn.Close()
		n.peers[i] = proto.NewAuctionClient(conn)
	}

	// bully: max id sends coordinator(m) to all on start
	// bids wait for mu until the backups have the deadline
	if n.id == n.primary {
		n.mu.Lock()
		n.state.Deadline = time.Now().Add(duration).UnixMilli()
		log.Printf("node %d: auction started, it ends at %s", n.id, time.UnixMilli(n.state.Deadline).Format("15:04:05"))
		go func() {
			defer n.mu.Unlock()
			for i, peer := range n.peers {
				if peer == nil {
					continue
				}
				// wait for the node to start instead of failing
				_, err := peer.Coordinator(context.Background(), n.state, grpc.WaitForReady(true))
				if err != nil {
					log.Fatalf("node %d: failed to send coordinator: %v", n.id, err)
				}
				log.Printf("node %d: ack from node %d", n.id, i)
			}
		}()
	}

	// server part
	listener, err := net.Listen("tcp", addresses[id])
	if err != nil {
		log.Fatalf("node %d: failed to listen: %v", id, err)
	}
	grpcServer := grpc.NewServer()
	proto.RegisterAuctionServer(grpcServer, n)
	log.Printf("node %d: listening on %s", id, addresses[id])
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("node %d: failed to serve: %v", id, err)
	}
}
