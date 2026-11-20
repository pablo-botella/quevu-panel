package qpanel

import (
	"fmt"
	"sync"

	quevu "github.com/pablo-botella/quevu-kernel"
)

// feed is the screen's end of a quevu.Feed: it owns both lanes and the
// goroutine that puts every delivery on the data lane, so deliveries come
// out in the order the screen received them.
type feed struct {
	scr   *Screen
	limit int
	data  chan quevu.Delivery
	req   chan quevu.Request
	wake  chan struct{}
	done  chan struct{}
	once  sync.Once

	mu        sync.Mutex
	following bool
	full      bool
	pending   []quevu.Chunk
}

func newFeed(s *Screen, buffer int) *feed {
	if buffer <= 0 {
		buffer = 1024
	}
	f := &feed{
		scr:   s,
		limit: buffer,
		data:  make(chan quevu.Delivery),
		req:   make(chan quevu.Request, 16),
		wake:  make(chan struct{}, 1),
		done:  make(chan struct{}),
	}
	go f.run()
	return f
}

func (f *feed) stop() {
	f.once.Do(func() { close(f.done) })
}

// push offers a live chunk to the feed. It never blocks: when the buffer
// of the feed is full the feed pauses itself and forgets what it held.
func (f *feed) push(c quevu.Chunk) {
	f.mu.Lock()
	if !f.following {
		f.mu.Unlock()
		return
	}
	if len(f.pending) >= f.limit {
		f.following = false
		f.pending = nil
		f.full = true
	} else {
		f.pending = append(f.pending, c)
	}
	f.mu.Unlock()
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

// setFollowing switches the live output on or off and forgets anything
// that was waiting.
func (f *feed) setFollowing(on bool) {
	f.mu.Lock()
	f.following = on
	f.pending = nil
	f.full = false
	f.mu.Unlock()
}

func (f *feed) run() {
	defer close(f.data)
	for {
		select {
		case <-f.done:
			return
		case r := <-f.req:
			if !f.send(f.handle(r)) {
				return
			}
		case <-f.wake:
			f.mu.Lock()
			batch, full := f.pending, f.full
			f.pending, f.full = nil, false
			f.mu.Unlock()
			switch {
			case full:
				if !f.send(quevu.Delivery{Op: quevu.OpPause, Full: true}) {
					return
				}
			case len(batch) > 0:
				if !f.send(quevu.Delivery{Op: quevu.OpLive, Chunks: batch}) {
					return
				}
			}
		}
	}
}

func (f *feed) send(d quevu.Delivery) bool {
	select {
	case f.data <- d:
		return true
	case <-f.done:
		return false
	}
}

func (f *feed) handle(r quevu.Request) quevu.Delivery {
	d := quevu.Delivery{Op: r.Op}
	switch r.Op {
	case quevu.OpFollow:
		d.Chunks = f.scr.follow(f, r.N)
	case quevu.OpPause:
		f.setFollowing(false)
	case quevu.OpTail:
		d.Chunks, d.Err = f.scr.Tail(r.N)
	case quevu.OpBefore:
		d.Chunks, d.Err = f.scr.Before(r.Seq, r.N)
	case quevu.OpAfter:
		d.Chunks, d.Err = f.scr.After(r.Seq, r.N)
	default:
		d.Err = fmt.Errorf("qpanel: unknown request op %d", int(r.Op))
	}
	return d
}
