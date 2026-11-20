package qpanel

import (
	"fmt"
	"sync"
	"testing"
	"time"

	quevu "github.com/pablo-botella/quevu-kernel"
)

// writer feeds consumers the way a log does: one batch at a time, with
// positions and times, to every consumer it was given.
type writer struct {
	seq   uint64
	now   func() time.Time
	sinks []quevu.Consumer
}

func to(sinks ...quevu.Consumer) *writer {
	return &writer{now: time.Now, sinks: sinks}
}

func (w *writer) put(lines ...string) {
	batch := make([]quevu.Chunk, len(lines))
	for i, l := range lines {
		w.seq++
		batch[i] = quevu.Chunk{Seq: w.seq, Time: w.now(), Data: []byte(l)}
	}
	for _, s := range w.sinks {
		s.Write(batch)
	}
}

// lines writes "<prefix><from>" .. "<prefix><to>".
func (w *writer) lines(prefix string, from, to int) {
	for i := from; i <= to; i++ {
		w.put(fmt.Sprintf("%s%d", prefix, i))
	}
}

func texts(cs []quevu.Chunk) []string {
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = string(c.Data)
	}
	return out
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func recv(t *testing.T, f *quevu.Feed) quevu.Delivery {
	t.Helper()
	select {
	case d, ok := <-f.Data:
		if !ok {
			t.Fatal("feed closed")
		}
		return d
	case <-time.After(2 * time.Second):
		t.Fatal("no delivery")
	}
	return quevu.Delivery{}
}

func TestMemoryIsBoundedAndPositional(t *testing.T) {
	s := New("t", WithMemory(5))
	to(s).lines("l", 1, 12)
	tail, _ := s.Tail(100)
	if want := []string{"l8", "l9", "l10", "l11", "l12"}; !equal(texts(tail), want) {
		t.Fatalf("tail %q", texts(tail))
	}
	before, _ := s.Before(11, 2)
	if want := []string{"l9", "l10"}; !equal(texts(before), want) {
		t.Fatalf("before %q", texts(before))
	}
	after, _ := s.After(9, 2)
	if want := []string{"l10", "l11"}; !equal(texts(after), want) {
		t.Fatalf("after %q", texts(after))
	}
	// The short memory ends at 8: nothing before it.
	if gone, _ := s.Before(8, 3); len(gone) != 0 {
		t.Fatalf("expected the end of the memory, got %q", texts(gone))
	}
	if none, _ := s.After(12, 3); len(none) != 0 {
		t.Fatalf("after the end: %q", texts(none))
	}
	if s.Name() != "t" {
		t.Fatalf("name %q", s.Name())
	}
}

func TestAScreenWithNoMemoryStillShowsLiveOutput(t *testing.T) {
	s := New("t", WithMemory(0))
	w := to(s)
	w.put("a")
	f := s.Subscribe(0)
	defer f.Close()
	f.Req <- quevu.Request{Op: quevu.OpFollow, N: 10}
	if d := recv(t, f); d.Op != quevu.OpFollow || len(d.Chunks) != 0 {
		t.Fatalf("follow: %+v", d)
	}
	w.put("b")
	if d := recv(t, f); d.Op != quevu.OpLive || !equal(texts(d.Chunks), []string{"b"}) {
		t.Fatalf("live: %+v", d)
	}
}

func TestFeedIsBornPausedAndFollows(t *testing.T) {
	s := New("t")
	w := to(s)
	w.put("a", "b", "c")
	f := s.Subscribe(0)
	defer f.Close()

	// Born paused: output written now is not delivered.
	w.put("d")
	select {
	case d := <-f.Data:
		t.Fatalf("unexpected delivery %+v", d)
	case <-time.After(30 * time.Millisecond):
	}

	f.Req <- quevu.Request{Op: quevu.OpFollow, N: 2}
	d := recv(t, f)
	if d.Op != quevu.OpFollow || !equal(texts(d.Chunks), []string{"c", "d"}) {
		t.Fatalf("follow: %v %q", d.Op, texts(d.Chunks))
	}
	w.put("e")
	d = recv(t, f)
	if d.Op != quevu.OpLive || !equal(texts(d.Chunks), []string{"e"}) {
		t.Fatalf("live: %v %q", d.Op, texts(d.Chunks))
	}
}

func TestFeedPauseAndResumeJumpsToTheEnd(t *testing.T) {
	s := New("t")
	w := to(s)
	f := s.Subscribe(0)
	defer f.Close()
	f.Req <- quevu.Request{Op: quevu.OpFollow, N: 10}
	recv(t, f)

	f.Req <- quevu.Request{Op: quevu.OpPause}
	if d := recv(t, f); d.Op != quevu.OpPause || d.Full {
		t.Fatalf("pause: %+v", d)
	}
	w.lines("l", 1, 50)
	// While paused the console can still move by position.
	f.Req <- quevu.Request{Op: quevu.OpBefore, Seq: 4, N: 2}
	if d := recv(t, f); d.Op != quevu.OpBefore || !equal(texts(d.Chunks), []string{"l2", "l3"}) {
		t.Fatalf("before: %v %q", d.Op, texts(d.Chunks))
	}
	// Resume does not catch up: it asks for the end.
	f.Req <- quevu.Request{Op: quevu.OpFollow, N: 3}
	if d := recv(t, f); d.Op != quevu.OpFollow || !equal(texts(d.Chunks), []string{"l48", "l49", "l50"}) {
		t.Fatalf("resume: %v %q", d.Op, texts(d.Chunks))
	}
}

func TestSlowConsoleGetsDoNotDisturbAndNeverHoldsTheScreen(t *testing.T) {
	s := New("t")
	w := to(s)
	f := s.Subscribe(8)
	defer f.Close()
	f.Req <- quevu.Request{Op: quevu.OpFollow, N: 0}
	recv(t, f)

	// The console stops reading. Whoever writes must not notice.
	done := make(chan struct{})
	go func() {
		w.lines("l", 0, 4999)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a console that does not read held up the screen")
	}

	// Sooner or later the sign shows up on the data lane.
	sawSign := false
	for i := 0; i < 5 && !sawSign; i++ {
		d := recv(t, f)
		sawSign = d.Op == quevu.OpPause && d.Full
	}
	if !sawSign {
		t.Fatal("no do-not-disturb sign")
	}
	// Nothing more comes until the console resumes.
	select {
	case d := <-f.Data:
		t.Fatalf("delivery while paused: %+v", d.Op)
	case <-time.After(30 * time.Millisecond):
	}
	f.Req <- quevu.Request{Op: quevu.OpFollow, N: 1}
	if d := recv(t, f); d.Op != quevu.OpFollow || !equal(texts(d.Chunks), []string{"l4999"}) {
		t.Fatalf("resume: %v %q", d.Op, texts(d.Chunks))
	}
}

func TestCloseEndsFeedsAndKeepsTheMemory(t *testing.T) {
	s := New("t")
	to(s).put("last words")
	f := s.Subscribe(0)
	s.Close()
	select {
	case _, ok := <-f.Data:
		if ok {
			t.Fatal("delivery after close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("feed not closed")
	}
	f.Close()
	f.Close()
	// Closed twice, as it is in practice: by its log and by the panel.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Write([]quevu.Chunk{{Seq: 9}}); err != ErrClosed {
		t.Fatalf("write after close: %v", err)
	}
	if got, _ := s.Tail(5); !equal(texts(got), []string{"last words"}) {
		t.Fatalf("memory after close: %q", texts(got))
	}
	// A console that arrives late gets a feed that has already ended.
	late := s.Subscribe(0)
	if _, ok := <-late.Data; ok {
		t.Fatal("a feed on a closed screen delivered")
	}
}

type fixedStats []quevu.ConsumerStats

func (f fixedStats) Stats() []quevu.ConsumerStats { return f }

func TestStatsArePassedOn(t *testing.T) {
	s := New("t")
	if s.Stats() != nil {
		t.Fatal("stats from nowhere")
	}
	s.ShowStats(fixedStats{{Queued: 3, Capacity: 10}})
	if st := s.Stats(); len(st) != 1 || st[0].Queued != 3 {
		t.Fatalf("stats %+v", st)
	}
}

// store is a persistent consumer and its stored-data provider in one:
// everything it was ever given, served by position.
type store struct {
	mu     sync.Mutex
	chunks []quevu.Chunk
	asked  int
}

func (s *store) Write(b []quevu.Chunk) error {
	s.mu.Lock()
	s.chunks = append(s.chunks, b...)
	s.mu.Unlock()
	return nil
}

func (s *store) Close() error { return nil }

func (s *store) Tail(n int) ([]quevu.Chunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.asked++
	return append([]quevu.Chunk(nil), s.chunks[max(len(s.chunks)-n, 0):]...), nil
}

func (s *store) Before(seq uint64, n int) ([]quevu.Chunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.asked++
	end := 0
	for end < len(s.chunks) && s.chunks[end].Seq < seq {
		end++
	}
	return append([]quevu.Chunk(nil), s.chunks[max(end-n, 0):end]...), nil
}

func (s *store) After(seq uint64, n int) ([]quevu.Chunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.asked++
	start := 0
	for start < len(s.chunks) && s.chunks[start].Seq <= seq {
		start++
	}
	return append([]quevu.Chunk(nil), s.chunks[start:min(start+n, len(s.chunks))]...), nil
}

func (s *store) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.asked
}
