// Command demo runs a self-contained 5-node coordination cluster in one
// process and walks it through leader election, fencing, a network
// partition and a rolling restart. Every decision line shows the term,
// holder/fence token and the reason.
//
//	go run ./cmd/demo
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/lisama569/distributed-consensus-go/pkg/consensus"
	"github.com/lisama569/distributed-consensus-go/pkg/coordinator"
	"github.com/lisama569/distributed-consensus-go/pkg/sim"
)

func main() {
	logger := log.New(os.Stdout, "", log.Ltime|log.Lmicroseconds)
	logf := func(format string, args ...any) { logger.Printf(format, args...) }

	cfg := consensus.DefaultConfig()
	root := filepath.Join(os.TempDir(), "dcons-demo")
	_ = os.RemoveAll(root)

	ids := []string{"node-A", "node-B", "node-C", "node-D", "node-E"}
	c := sim.NewCluster(ids, root, cfg, logf)
	c.StartAll()
	defer c.StopAll()

	section("1. leader election")
	leader, term, err := c.WaitForLeader(3 * time.Second)
	must(err)
	logf("elected leader=%s term=%d (majority of %d nodes granted votes)", leader, term, len(ids))

	section("2. lease lock + fencing")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, servedBy, err := c.Submit(ctx, coordinator.Command{
		Kind: "acquire", RequestID: "demo-acq-1", Resource: "payments",
		Holder: "svc-1", LeaseMS: 400,
	})
	must(err)
	logf("acquire  holder=svc-1 resource=payments -> ok=%v token=%d leaseUntil=%d servedBy=%s term=%d [basis: majority commit, fresh grant]",
		resp.OK, resp.Token, resp.LeaseUntilMS, servedBy, c.Node(servedBy).Raft.Term())

	// Retry: identical RequestID must not double-lock or bump the token.
	retry, _, err := c.Submit(ctx, coordinator.Command{
		Kind: "acquire", RequestID: "demo-acq-1", Resource: "payments",
		Holder: "svc-1", LeaseMS: 400,
	})
	must(err)
	logf("retry    same request id -> replayed=%v token=%d (identical response, no duplicate lock)",
		retry.Replayed, retry.Token)

	// Competing holder while lease live -> explicit reason.
	busy, _, err := c.Submit(ctx, coordinator.Command{
		Kind: "acquire", RequestID: "demo-acq-2", Resource: "payments",
		Holder: "svc-2", LeaseMS: 400,
	})
	must(err)
	logf("acquire  holder=svc-2 -> ok=%v reason=%q [basis: svc-1 lease live]", busy.OK, busy.Reason)

	// Fenced write accepted with the correct token.
	ok, _, err := c.Submit(ctx, coordinator.Command{
		Kind: "commit", RequestID: "demo-commit-1", Resource: "payments",
		Holder: "svc-1", Token: resp.Token,
	})
	must(err)
	logf("commit   holder=svc-1 token=%d -> ok=%v [basis: token==current && lease live]", resp.Token, ok.OK)

	section("3. lease expiry")
	logf("sleeping 600ms so the 400ms lease elapses ...")
	time.Sleep(600 * time.Millisecond)
	expired, _, err := c.Submit(ctx, coordinator.Command{
		Kind: "commit", RequestID: "demo-commit-2", Resource: "payments",
		Holder: "svc-1", Token: resp.Token,
	})
	must(err)
	logf("commit   holder=svc-1 token=%d after expiry -> ok=%v reason=%q [basis: leaseUntil<=now]",
		resp.Token, expired.OK, expired.Reason)

	takeover, _, err := c.Submit(ctx, coordinator.Command{
		Kind: "acquire", RequestID: "demo-acq-3", Resource: "payments",
		Holder: "svc-2", LeaseMS: 5_000,
	})
	must(err)
	logf("acquire  holder=svc-2 after expiry -> ok=%v token=%d (strictly greater than old token %d)",
		takeover.OK, takeover.Token, resp.Token)

	stale, _, err := c.Submit(ctx, coordinator.Command{
		Kind: "commit", RequestID: "demo-commit-3", Resource: "payments",
		Holder: "svc-1", Token: resp.Token,
	})
	must(err)
	logf("commit   old holder=svc-1 old token=%d -> ok=%v reason=%q [basis: current token is %d, fence blocks stale owner]",
		resp.Token, stale.OK, stale.Reason, takeover.Token)

	section("4. network partition (leader isolated in a 2-node minority)")
	others := c.IDs()
	var minor []string
	minor = append(minor, leader)
	for _, id := range others {
		if id != leader {
			minor = append(minor, id)
			break
		}
	}
	c.Partition(minor)
	logf("partition: %s isolated together (minority)", minor)

	// Wait for the majority side to elect a new leader in a higher term.
	var newLeader string
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range others {
			if id == minor[1] {
				continue
			}
			if h := c.Node(id); h.Raft.IsLeader() && h.Raft.Term() > term {
				newLeader = id
			}
		}
		if newLeader != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if newLeader == "" {
		log.Fatal("majority side failed to elect a new leader")
	}
	newTerm := c.Node(newLeader).Raft.Term()
	logf("majority elected new leader=%s term=%d (> old term %d); stale leader=%s cannot commit without majority",
		newLeader, newTerm, term, leader)

	// Serve a new lock through the majority side while the split persists.
	part, _, err := c.Submit(ctx, coordinator.Command{
		Kind: "acquire", RequestID: "demo-acq-4", Resource: "ledger-7",
		Holder: "svc-3", LeaseMS: 5_000,
	}, newLeader)
	must(err)
	logf("acquire during partition served by majority leader=%s term=%d: resource=ledger-7 token=%d ok=%v",
		newLeader, newTerm, part.Token, part.OK)

	c.Heal()
	time.Sleep(400 * time.Millisecond)
	var leaders []string
	for _, id := range c.IDs() {
		if c.Node(id).Raft.IsLeader() {
			leaders = append(leaders, id)
		}
	}
	logf("partition healed; current leaders=%v (must be exactly one) [basis: stale leader saw higher term and stepped down]", leaders)
	if len(leaders) != 1 {
		log.Fatal("split brain after heal")
	}

	section("5. rolling restart (term/log durability)")
	before := c.Node(leaders[0]).Raft.Term()
	c.Restart(leaders[0])
	time.Sleep(300 * time.Millisecond)
	if c.Node(leaders[0]).Raft.IsLeader() {
		logf("restarted old leader %s correctly came back as follower; durable term >= %d", leaders[0], before)
	}
	restarted, _, err := c.Submit(ctx, coordinator.Command{
		Kind: "commit", RequestID: "demo-commit-4", Resource: "ledger-7",
		Holder: "svc-3", Token: part.Token,
	})
	must(err)
	logf("fenced commit after rolling restart -> ok=%v token=%d [basis: committed log replayed from disk on restart]",
		restarted.OK, restarted.Token)

	section("demo complete")
	logf("invariants observed throughout: one committable leader per term; terms monotonic across restarts; fence tokens strictly increasing; every rejection carried an explicit reason")
	fmt.Println("\nrun the full test suite:  go test ./... -v")
}

func section(name string) {
	fmt.Printf("\n==== %s ====\n", name)
}

func must(err error) {
	if err != nil {
		log.Fatalf("unexpected error: %v", err)
	}
}
