package model

import "testing"

func nodeWith(server string, delay int, speed float64, alive bool) *Node {
	n := New(map[string]any{
		"name":   server,
		"type":   "ss",
		"server": server,
		"port":   443,
	})
	n.Delay = delay
	n.Speed = speed
	n.Alive = alive
	return n
}

func TestAssignSpeedRanks(t *testing.T) {
	fast := nodeWith("fast", 300, 12.0, true)
	mid := nodeWith("mid", 100, 5.5, true)
	slow := nodeWith("slow", 50, 1.2, true)
	untested := nodeWith("untested", 80, 0, true)
	dead := nodeWith("dead", -1, 99, false)

	nodes := []*Node{mid, slow, fast, untested, dead}
	AssignSpeedRanks(nodes)

	if fast.SpeedRank != 1 || mid.SpeedRank != 2 || slow.SpeedRank != 3 {
		t.Fatalf("排行错误: fast=%d mid=%d slow=%d", fast.SpeedRank, mid.SpeedRank, slow.SpeedRank)
	}
	if untested.SpeedRank != 0 {
		t.Fatalf("未测出速度的节点排行应为 0，实际 %d", untested.SpeedRank)
	}
	// 不可用但残留速度的节点同样不参与排行
	if dead.SpeedRank != 0 {
		t.Fatalf("不可用节点排行应为 0，实际 %d", dead.SpeedRank)
	}
}

func TestAssignSpeedRanksTiesShareRank(t *testing.T) {
	a := nodeWith("a", 10, 3.0, true)
	b := nodeWith("b", 20, 3.0, true)
	c := nodeWith("c", 30, 1.0, true)
	nodes := []*Node{a, b, c}
	AssignSpeedRanks(nodes)
	if a.SpeedRank != 1 || b.SpeedRank != 1 {
		t.Fatalf("同速度应共享名次: a=%d b=%d", a.SpeedRank, b.SpeedRank)
	}
	if c.SpeedRank != 3 {
		t.Fatalf("同分后名次应跳号: c=%d", c.SpeedRank)
	}
}

func TestSortBySpeed(t *testing.T) {
	slow := nodeWith("slow", 30, 1.0, true)
	fast := nodeWith("fast", 400, 20.0, true)
	untested := nodeWith("untested", 20, 0, true)
	dead := nodeWith("dead", 10, 50.0, false)
	nodes := []*Node{untested, slow, dead, fast}
	AssignSpeedRanks(nodes)
	SortBySpeed(nodes)

	want := []string{"fast", "slow", "untested", "dead"}
	for i, name := range want {
		if nodes[i].Name() != name {
			t.Fatalf("第 %d 个应为 %s，实际 %s（顺序 %v）", i+1, name, nodes[i].Name(), namesOf(nodes))
		}
	}
}

func namesOf(nodes []*Node) []string {
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.Name())
	}
	return out
}
