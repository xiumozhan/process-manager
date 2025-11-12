package main

import (
	"fmt"
	"sync"
	"time"
)

type logs struct {
	buf    []byte
	mu     sync.RWMutex
	cond   *sync.Cond
	closed bool
}

func newLogs() *logs {
	l := &logs{
		buf: make([]byte, 0, 1024),
	}
	l.cond = sync.NewCond(l.mu.RLocker())
	return l
}

func (h *logs) write(chunk []byte) {
	h.mu.Lock()
	h.buf = append(h.buf, chunk...)
	h.mu.Unlock()
	h.cond.Broadcast()
}

func (h *logs) close() {
	h.mu.Lock()
	h.closed = true
	h.mu.Unlock()
	h.cond.Broadcast()
}

func (h *logs) waitForCurrentSize() (int, bool) {
	h.mu.RLock()
	// If already closed, return immediately
	if h.closed {
		size := len(h.buf)
		h.mu.RUnlock()
		return size, true
	}
	h.cond.Wait()
	size := len(h.buf)
	closed := h.closed
	h.mu.RUnlock()
	return size, closed
}

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
	defer func() {
		h.close()
		close(quitComplete)
	}()

	for {
		select {
		case <-quit:
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
	var seen int
	totalRequests := 0

	defer func() {
		fmt.Printf("[sub %d] total requests=%d, total bytes read=%d\n", id, totalRequests, seen)
	}()

	for {
		select {
		case <-quit:
			fmt.Printf("[sub %d] quit via channel\n", id)
			return
		default:
		}

		currentSize, closed := h.waitForCurrentSize()

		if currentSize <= seen {
			// No new data
			if closed {
				// EOF reached and all data consumed
				fmt.Printf("[sub %d] EOF reached, terminating\n", id)
				return
			}
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

		// After reading, check if we're at EOF
		if closed && seen >= total {
			fmt.Printf("[sub %d] all data read after EOF, terminating\n", id)
			return
		}
	}
}

func main() {
	h := newLogs()

	quitWriter := make(chan struct{})
	quitWriterComplete := make(chan struct{})
	quitSubChans := make([]chan struct{}, 15)

	defer func() {
		for _, ch := range quitSubChans {
			close(ch)
		}
		fmt.Printf("total len=%d\n", len(h.buf))
	}()

	go writer(h, quitWriter, quitWriterComplete)

	// Start first 10 subscribers immediately
	for i := 0; i < 10; i++ {
		quitSubChans[i] = make(chan struct{})
		go subscriber(h, i, quitSubChans[i])
	}

	// Wait 2 seconds and start 3 more subscribers (these join halfway)
	time.Sleep(2 * time.Second)
	fmt.Println("=== Starting subscribers 10-12 halfway ===")
	for i := 10; i < 13; i++ {
		quitSubChans[i] = make(chan struct{})
		go subscriber(h, i, quitSubChans[i])
	}

	// Wait 3 more seconds and stop the writer (trigger EOF)
	time.Sleep(3 * time.Second)
	fmt.Println("=== Stopping writer (EOF) ===")
	close(quitWriter)
	<-quitWriterComplete

	// Wait a bit to see subscribers finish
	time.Sleep(1 * time.Second)

	// Start 2 more subscribers after EOF
	fmt.Println("=== Starting subscribers 13-14 after EOF ===")
	for i := 13; i < 15; i++ {
		quitSubChans[i] = make(chan struct{})
		go subscriber(h, i, quitSubChans[i])
	}

	// Wait for all subscribers to finish reading
	time.Sleep(3 * time.Second)
}
