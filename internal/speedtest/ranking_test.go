package speedtest

import (
	"testing"

	"proxypool/internal/config"
	"proxypool/internal/model"
)

func aliveNode(name string, delay int) *model.Node {
	n := model.New(map[string]any{
		"name":   name,
		"type":   "ss",
		"server": name,
		"port":   443,
	})
	n.Delay, n.Alive = delay, true
	return n
}

func TestDownloadCandidatesTop(t *testing.T) {
	nodes := []*model.Node{
		aliveNode("d500", 500),
		aliveNode("d100", 100),
		aliveNode("d300", 300),
		aliveNode("d200", 200),
	}
	dead := aliveNode("dead", 1)
	dead.Alive = false
	nodes = append(nodes, dead)

	cfg := config.SpeedTestConfig{MaxDelay: 1000, Download: config.DownloadConfig{Top: 2}}
	tester := NewCore(cfg, "", nil)
	got := tester.downloadCandidates(nodes)
	if len(got) != 2 {
		t.Fatalf("候选数应为 2，实际 %d", len(got))
	}
	if got[0].Name() != "d100" || got[1].Name() != "d200" {
		t.Fatalf("候选应按延迟从低到高截取: %s, %s", got[0].Name(), got[1].Name())
	}
}

func TestDownloadCandidatesTopPercent(t *testing.T) {
	nodes := make([]*model.Node, 0, 10)
	for i := 0; i < 10; i++ {
		nodes = append(nodes, aliveNode("n", (i+1)*10))
	}
	cfg := config.SpeedTestConfig{Download: config.DownloadConfig{TopPercent: 30}}
	got := NewCore(cfg, "", nil).downloadCandidates(nodes)
	if len(got) != 3 {
		t.Fatalf("10 个节点的 30%% 应为 3 个，实际 %d", len(got))
	}

	// 同时配置 top 与 top_percent 时取更大的候选集
	cfg.Download.Top = 5
	got = NewCore(cfg, "", nil).downloadCandidates(nodes)
	if len(got) != 5 {
		t.Fatalf("应取 top 与 top_percent 中更大的候选集，实际 %d", len(got))
	}
}

func TestDownloadCandidatesFiltersByMaxDelay(t *testing.T) {
	nodes := []*model.Node{aliveNode("slow", 2000), aliveNode("ok", 100)}
	cfg := config.SpeedTestConfig{MaxDelay: 1000}
	got := NewCore(cfg, "", nil).downloadCandidates(nodes)
	if len(got) != 1 || got[0].Name() != "ok" {
		t.Fatalf("超过延迟阈值的节点不应进入下载测速: %v", got)
	}
}
