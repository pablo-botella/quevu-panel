// Package qpanel is the panel as a writer of a log.
//
// A Screen is attached to a log like any other writer, next to the
// file or the database. It is the volatile one: it keeps the last few
// hundred chunks in memory, hands new ones to whoever is watching, and
// stores nothing. When the application ends, what a screen held is gone.
//
// To the console that shows it, a screen is two things at once:
//
//   - the live output, through a feed that is never waited for: a
//     console that falls behind is paused, not the log;
//   - the immediate provider: what was written a moment ago, given back
//     by position without touching any store.
//
// What lies further back is not the screen's business. The providers the
// application attaches (see quevu.Join) take over where the short memory
// ends: an immediate one by itself, a historical one only when the user
// asks for it.
//
// The log knows none of this. To the log a screen is a writer that
// happens to be fast.
package qpanel

import (
	"errors"
	"sync"

	quevu "github.com/pablo-botella/quevu-kernel"
)

// DefaultMemory is how many chunks a screen remembers: a few screenfuls
// of a large display.
const DefaultMemory = 500

// ErrClosed is returned by a Write to a closed screen.
var ErrClosed = errors.New("qpanel: screen closed")

// Screen is the volatile writer of one log, and what a console shows
// in one tab.
type Screen struct {
	fails quevu.Failures // the body of Run, and what it could not write

	name string

	mu     sync.Mutex
	ring   *ring
	feeds  map[*feed]struct{}
	closed bool
	stats  quevu.StatSource
}

// Option configures a Screen.
type Option func(*Screen)

// WithMemory sets how many chunks the screen remembers. Zero keeps none:
// the console then gets live output only.
func WithMemory(chunks int) Option {
	return func(s *Screen) { s.ring = newRing(chunks) }
}

// New creates the screen of a log. name is what its tab is titled with.
func New(name string, opts ...Option) *Screen {
	s := &Screen{
		name:  name,
		ring:  newRing(DefaultMemory),
		feeds: make(map[*feed]struct{}),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Name identifies the screen.
func (s *Screen) Name() string { return s.name }

// Write takes a batch from the log: it is remembered and offered to
// every console watching. It never waits for a console.
func (s *Screen) Write(batch []quevu.Chunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	for _, c := range batch {
		s.ring.add(c)
		for f := range s.feeds {
			f.push(c)
		}
	}
	return nil
}

// Close ends the feeds. What the screen remembers stays readable, so a
// window still open shows the last words of a log that has closed. It is
// safe to call more than once: both the log and the panel close a screen.
func (s *Screen) Close() error {
	s.mu.Lock()
	feeds := s.feeds
	s.feeds = make(map[*feed]struct{})
	s.closed = true
	s.mu.Unlock()
	for f := range feeds {
		f.stop()
	}
	return nil
}

// Tail returns the last n chunks the screen remembers.
func (s *Screen) Tail(n int) ([]quevu.Chunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ring.tail(n), nil
}

// Before returns up to n remembered chunks that precede seq. An empty
// answer means the short memory ends there: further back is the business
// of a stored-data provider.
func (s *Screen) Before(seq uint64, n int) ([]quevu.Chunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ring.before(seq, n), nil
}

// After returns up to n remembered chunks that follow seq.
func (s *Screen) After(seq uint64, n int) ([]quevu.Chunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ring.after(seq, n), nil
}

// Subscribe opens a feed for a console. buffer is how many live chunks
// may wait for the console before the screen pauses the feed by itself;
// zero or less means 1024.
func (s *Screen) Subscribe(buffer int) *quevu.Feed {
	f := newFeed(s, buffer)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		f.stop()
	} else {
		s.feeds[f] = struct{}{}
		s.mu.Unlock()
	}
	return quevu.NewFeed(f.data, f.req, func() {
		s.mu.Lock()
		delete(s.feeds, f)
		s.mu.Unlock()
		f.stop()
	})
}

// follow takes the last n chunks and switches the feed to live output in
// one step, so nothing falls between the two.
func (s *Screen) follow(f *feed, n int) []quevu.Chunk {
	s.mu.Lock()
	defer s.mu.Unlock()
	f.setFollowing(true)
	return s.ring.tail(n)
}

// ShowStats tells the screen where to read the queues of its log from,
// so the console can show them: a queue that keeps growing is how one
// sees that a writer is too slow. The screen only passes them on.
func (s *Screen) ShowStats(from quevu.StatSource) {
	s.mu.Lock()
	s.stats = from
	s.mu.Unlock()
}

// Stats returns the queues of the log, if the screen was told where to
// find them.
func (s *Screen) Stats() []quevu.WriterStats {
	s.mu.Lock()
	from := s.stats
	s.mu.Unlock()
	if from == nil {
		return nil
	}
	return from.Stats()
}

var (
	_ quevu.Writer   = (*Screen)(nil)
	_ quevu.Source     = (*Screen)(nil)
	_ quevu.StatSource = (*Screen)(nil)
)

// Run receives from the channel the hub gave this writer until it is
// closed, writing what arrives in batches, and closes the writer at the
// end. It is what makes it a quevu.Writer.
func (s *Screen) Run(in <-chan quevu.Chunk) error { return s.fails.Drain(in, 0, s) }

// Failed returns the chunks lost so far in failed batches, and the last
// error.
func (s *Screen) Failed() (uint64, error) { return s.fails.Failed() }
