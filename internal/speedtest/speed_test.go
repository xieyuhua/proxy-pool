package speedtest

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// newTestInstance 构造一个仅用于下载测速的 coreInstance：
// 控制面（选择节点）与数据面（混合端口）都指向本地测试服务器。
func newTestInstance(t *testing.T, dataSrv *httptest.Server) *coreInstance {
	t.Helper()
	ctrl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || !strings.HasPrefix(r.URL.Path, "/proxies/") {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(ctrl.Close)

	ctrlAddr := strings.TrimPrefix(ctrl.URL, "http://")
	port := 0
	if _, err := fmt.Sscanf(ctrlAddr, "127.0.0.1:%d", &port); err != nil {
		t.Fatalf("解析测试控制面地址失败: %v", err)
	}
	return &coreInstance{
		ctrlPort:  port,
		secret:    "test",
		apiClient: &http.Client{Timeout: 5 * time.Second},
		pxClient:  &http.Client{Timeout: 30 * time.Second},
		dlGroups:  []string{"__DL0__"},
	}
}

// slowBody 模拟限速下载：每 10ms 返回 64KB，总大小 totalBytes。
func slowBody(totalBytes int64) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", totalBytes))
		chunk := make([]byte, 64*1024)
		var sent int64
		flusher, _ := w.(http.Flusher)
		for sent < totalBytes {
			n := int64(len(chunk))
			if totalBytes-sent < n {
				n = totalBytes - sent
			}
			if _, err := w.Write(chunk[:n]); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			sent += n
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func TestSpeedRespectsMaxBytes(t *testing.T) {
	const total = 8 << 20 // 8MB
	srv := httptest.NewServer(slowBody(total))
	defer srv.Close()

	inst := newTestInstance(t, srv)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	speed, err := inst.Speed(ctx, "__DL0__", "node", srv.URL, SpeedOptions{
		Duration: 5 * time.Second,
		MaxBytes: 2 << 20, // 只读 2MB
	})
	if err != nil {
		t.Fatalf("测速失败: %v", err)
	}
	if speed <= 0 {
		t.Fatalf("速度应大于 0，实际 %v", speed)
	}
}

func TestSpeedReturnsValueForSmallBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(make([]byte, 128*1024))
	}))
	defer srv.Close()

	inst := newTestInstance(t, srv)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	speed, err := inst.Speed(ctx, "__DL0__", "node", srv.URL, SpeedOptions{Duration: 3 * time.Second, Warmup: time.Second})
	if err != nil {
		t.Fatalf("小文件也应返回速度而不是错误: %v", err)
	}
	if speed <= 0 {
		t.Fatalf("速度应大于 0，实际 %v", speed)
	}
}

func TestSpeedErrorsOnHTTPFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	inst := newTestInstance(t, srv)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := inst.Speed(ctx, "__DL0__", "node", srv.URL, SpeedOptions{Duration: time.Second}); err == nil {
		t.Fatal("HTTP 500 应返回错误")
	}
}

func TestSpeedRejectsSelectionFailure(t *testing.T) {
	ctrl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"message":"group not found"}`, http.StatusNotFound)
	}))
	defer ctrl.Close()

	inst := newTestInstance(t, httptest.NewServer(http.NotFoundHandler()))
	addr := strings.TrimPrefix(ctrl.URL, "http://")
	var port int
	if _, err := fmt.Sscanf(addr, "127.0.0.1:%d", &port); err != nil {
		t.Fatal(err)
	}
	inst.ctrlPort = port

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := inst.Speed(ctx, "__DL0__", "node", "http://127.0.0.1:1/", SpeedOptions{Duration: time.Second}); err == nil {
		t.Fatal("选择节点失败时应返回错误")
	}
}
