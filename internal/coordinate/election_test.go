package coordinate

import (
	"context"
	"io"
	"log"
	"testing"
	"time"
)

func newTestCluster(t *testing.T, n int, opts ...ClusterOption) *Cluster {
	t.Helper()
	quiet := log.New(io.Discard, "", 0)
	allOpts := append([]ClusterOption{WithLogger(quiet)}, opts...)
	c, err := NewCluster(n, allOpts...)
	if err != nil {
		t.Fatalf("new cluster: %v", err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	t.Cleanup(func() { _ = c.Stop() })
	return c
}

func countLeaders(c *Cluster) []int {
	var leaders []int
	for i := 0; i < c.Size(); i++ {
		if c.Node(i).Role() == Leader {
			leaders = append(leaders, i)
		}
	}
	return leaders
}

// aliveLeader 在未宕机、且满足 match 的节点中找唯一领导者。
func aliveLeader(c *Cluster, match func(i int) bool) int {
	for i := 0; i < c.Size(); i++ {
		if c.Network().IsDown(i) {
			continue
		}
		if match(i) && c.Node(i).Role() == Leader {
			return i
		}
	}
	return -1
}

// aliveLeadersExcept 返回除 excluded 外所有自认领导者的存活节点。
func aliveLeadersExcept(c *Cluster, excluded int) []int {
	var leaders []int
	for i := 0; i < c.Size(); i++ {
		if i == excluded || c.Network().IsDown(i) {
			continue
		}
		if c.Node(i).Role() == Leader {
			leaders = append(leaders, i)
		}
	}
	return leaders
}

func TestElection_SingleLeader(t *testing.T) {
	c := newTestCluster(t, 5)
	id, err := c.WaitForLeader(2 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if leaders := countLeaders(c); len(leaders) != 1 {
		t.Fatalf("leaders=%v, want exactly 1", leaders)
	}
	t.Logf("leader elected: node %d term=%d", id, c.Node(id).Term())
}

func TestElection_TermMonotonicAcrossTerms(t *testing.T) {
	c := newTestCluster(t, 3)
	id, err := c.WaitForLeader(2 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	term1 := c.Node(id).Term()

	// 干掉领导者，集群必须选出更高任期的新领导者。
	c.Network().SetDown(id, true)
	id2 := -1
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		id2 = aliveLeader(c, func(i int) bool { return i != id && c.Node(i).Term() > term1 })
		if id2 != -1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if id2 == -1 {
		t.Fatal("no new leader after killing old one")
	}
	term2 := c.Node(id2).Term()
	if term2 <= term1 {
		t.Fatalf("term went backwards: %d -> %d", term1, term2)
	}
	t.Logf("leadership moved node %d(term %d) -> node %d(term %d)", id, term1, id2, term2)
}

func TestElection_MinorityPartitionCannotElect(t *testing.T) {
	c := newTestCluster(t, 5)
	id, err := c.WaitForLeader(2 * time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// 把领导者单独隔离成少数派：{id} vs 其余 4 节点。
	leader := id
	c.Network().Partition(func(from, to int) bool {
		return from == leader || to == leader
	})

	// 多数派一侧应选出新领导者（旧主被隔离，本地仍可能自认 leader，
	// 但它联系不到多数派，是"无效"的旧主）。
	newLeader := -1
	termOld := c.Node(leader).Term()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		newLeader = aliveLeader(c, func(i int) bool {
			return i != leader && c.Node(i).Role() == Leader && c.Node(i).Term() > termOld
		})
		if newLeader != -1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if newLeader == -1 {
		t.Fatal("majority side failed to elect a new leader")
	}

	// 分区持续期间，只有多数派一侧能选出并维持领导者。
	time.Sleep(500 * time.Millisecond)
	majorityLeaders := aliveLeadersExcept(c, leader)
	if len(majorityLeaders) != 1 || majorityLeaders[0] != newLeader {
		t.Fatalf("majority-side leaders=%v, want only %d", majorityLeaders, newLeader)
	}

	// 恢复分区：旧领导者必须因更高任期退下，最终全集群只剩一个领导者。
	c.Network().Heal()
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(countLeaders(c)) == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if ls := countLeaders(c); len(ls) != 1 {
		t.Fatalf("after heal leaders=%v, want exactly 1", ls)
	}
	if c.Node(leader).Role() == Leader && c.Node(leader).Term() >= c.Node(newLeader).Term() {
		t.Fatal("old partitioned leader kept leadership with stale term after heal")
	}
	t.Logf("partition healed: sole leader node %d term=%d (old leader node %d stepped down)",
		c.LeaderID(), c.Node(c.LeaderID()).Term(), leader)
}
