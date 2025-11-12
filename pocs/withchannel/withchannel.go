package main

import (
	"fmt"
	"sync"
	"time"
)

type readReq struct {
	start int
	end   int
	reply chan []byte
}

type logs struct {
	writerCh chan []byte
	readCh   chan readReq

	subs map[chan int]struct{}
	buf  []byte

	mu sync.RWMutex
}

func newLogs() *logs {
	return &logs{
		writerCh: make(chan []byte),
		readCh:   make(chan readReq),
		subs:     make(map[chan int]struct{}),
		buf:      make([]byte, 0, 1024),
	}
}

func (h *logs) addSub(ch chan int) {
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
}

func (h *logs) rmSub(ch chan int) {
	h.mu.Lock()
	delete(h.subs, ch)
	h.mu.Unlock()
}

func (h *logs) write(chunk []byte) {
	h.mu.Lock()
	h.buf = append(h.buf, chunk...)
	newSize := len(h.buf)
	for ch := range h.subs {
		ch <- newSize
	}
	h.mu.Unlock()
}

// readRange is the public function that subscribers call.
// It proxies a read request to the logs and returns a copy of the requested range.
func (h *logs) readRange(start int) ([]byte, int) {
	h.mu.RLock()
	total := len(h.buf)
	out := make([]byte, total-start)
	copy(out, h.buf[start:])
	h.mu.RUnlock()
	return out, total
}

func writer(h *logs, quit <-chan struct{}, quitComplete chan struct{}) {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-quit:
			close(quitComplete)
			return
		case <-t.C:
			time.Sleep(10 * time.Millisecond)
		default:
		}
		n := 800
		chunk := make([]byte, n)
		for i := range chunk {
			chunk[i] = byte('A')
		}
		h.write(chunk)
	}
}

func subscriber(h *logs, id int, quit <-chan struct{}) {
	subCh := make(chan int, 10000)
	h.addSub(subCh)
	defer h.rmSub(subCh)
	defer close(subCh)

	var seen int
	totalRequests := 0

	defer func() {
		fmt.Printf("[sub %d] total requests=%d\n", id, totalRequests)
	}()

	for {
		select {
		case <-quit:
			return
		case size := <-subCh:
			if size <= seen {
				continue
			}
			data, total := h.readRange(seen)
			totalRequests++
			if len(data) > 0 {
				fmt.Printf("[sub %d] got %d new bytes (slice total=%d)\n",
					id, len(data), total)
			}
			seen += len(data)
			if seen != total {
				panic(fmt.Sprintf("[sub %d] total=%d, seen=%d\n", id, total, seen))
			}
		}
	}
}

func main() {
	h := newLogs()

	quitWriter := make(chan struct{})
	quitWriterComplete := make(chan struct{})
	quitSubChans := make([]chan struct{}, 15)

	defer func() {
		close(quitWriter)
		<-quitWriterComplete
		for _, ch := range quitSubChans {
			close(ch)
		}
		fmt.Printf("total len=%d\n", len(h.buf))
	}()

	go writer(h, quitWriter, quitWriterComplete)

	for i := 0; i < 15; i++ {
		quitSubChans[i] = make(chan struct{})
		go subscriber(h, i, quitSubChans[i])
	}

	time.Sleep(60 * time.Second)
}
