package qpanel

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/pablo-botella/quevu"
)

// These tests are about roles: the same kind of provider (a store) is
// set up several times, and what each one is to the console depends only
// on where the configuration puts it.

// holding returns a store that holds lines from..to of one log.
func holding(from, to uint64) *store {
	s := &store{}
	for i := from; i <= to; i++ {
		s.Write([]quevu.Chunk{{Seq: i, Time: time.Now(), Data: fmt.Appendf(nil, "l%d", i)}})
	}
	return s
}

func seqs(cs []quevu.Chunk) string {
	if len(cs) == 0 {
		return ""
	}
	if len(cs) == 1 {
		return fmt.Sprint(cs[0].Seq)
	}
	for i := 1; i < len(cs); i++ {
		if cs[i].Seq != cs[i-1].Seq+1 {
			out := ""
			for _, c := range cs {
				out += fmt.Sprint(c.Seq) + " "
			}
			return "NOT CONSECUTIVE: " + out
		}
	}
	return fmt.Sprintf("%d..%d", cs[0].Seq, cs[len(cs)-1].Seq)
}

// threeLevels is a log of 100 lines as a console finds it:
//
//	the screen remembers 91..100                  (its own memory)
//	the live store holds 51..100                  (immediate)
//	two archives hold 1..20 and 21..50            (historical, aggregated)
func threeLevels() (quevu.Source, *store, *store, *store) {
	scr := New("t", WithMemory(10))
	for i := uint64(1); i <= 100; i++ {
		scr.Write([]quevu.Chunk{{Seq: i, Time: time.Now(), Data: fmt.Appendf(nil, "l%d", i)}})
	}
	live := holding(51, 100)
	old, older := holding(21, 50), holding(1, 20)
	src := quevu.Join(scr, quevu.Roles{
		Immediate:  live,
		Historical: quevu.Aggregate(older, old),
	})
	return src, live, old, older
}

func ask(t *testing.T, f *quevu.Feed, r quevu.Request) quevu.Delivery {
	t.Helper()
	f.Req <- r
	d := recv(t, f)
	if d.Op != r.Op {
		t.Fatalf("asked %v, answered %v", r.Op, d.Op)
	}
	return d
}

func TestTheImmediateProviderIsAskedWithoutAsking(t *testing.T) {
	src, live, old, older := threeLevels()
	// Past the screen's memory the immediate provider answers by itself.
	got, _ := src.Before(95, 10)
	if seqs(got) != "85..94" || live.calls() == 0 {
		t.Fatalf("got %s, immediate asked %d times", seqs(got), live.calls())
	}
	// Nothing, ever, reaches the history through an ordinary request.
	got, _ = src.Before(60, 100)
	if seqs(got) != "51..59" {
		t.Fatalf("got %s", seqs(got))
	}
	if got, _ = src.Before(51, 100); len(got) != 0 {
		t.Fatalf("went into the history: %s", seqs(got))
	}
	if got, _ = src.Tail(1000); seqs(got) != "51..100" {
		t.Fatalf("tail: %s", seqs(got))
	}
	if old.calls()+older.calls() != 0 {
		t.Fatal("the history was queried without the user asking")
	}
}

func TestTheHistoryIsOfferedAndOnlyGivenOnRequest(t *testing.T) {
	src, _, old, older := threeLevels()
	f := src.Subscribe(0)
	defer f.Close()
	ask(t, f, quevu.Request{Op: quevu.OpFollow, N: 5})
	ask(t, f, quevu.Request{Op: quevu.OpPause})

	// Scrolling back reaches the end of recent output: the answer comes
	// back short, with the offer.
	d := ask(t, f, quevu.Request{Op: quevu.OpBefore, Seq: 55, N: 10})
	if seqs(d.Chunks) != "51..54" || !d.History {
		t.Fatalf("got %s, offer=%v", seqs(d.Chunks), d.History)
	}
	d = ask(t, f, quevu.Request{Op: quevu.OpBefore, Seq: 51, N: 10})
	if len(d.Chunks) != 0 || !d.History {
		t.Fatalf("got %s, offer=%v", seqs(d.Chunks), d.History)
	}
	// A full answer carries no offer: there is more where that came from.
	d = ask(t, f, quevu.Request{Op: quevu.OpBefore, Seq: 90, N: 10})
	if seqs(d.Chunks) != "80..89" || d.History {
		t.Fatalf("got %s, offer=%v", seqs(d.Chunks), d.History)
	}
	if old.calls()+older.calls() != 0 {
		t.Fatal("the history was queried before the user asked")
	}

	// The user asks. The console never learns it is two archives.
	d = ask(t, f, quevu.Request{Op: quevu.OpHistoryBefore, Seq: 51, N: 5})
	if seqs(d.Chunks) != "46..50" || d.History {
		t.Fatalf("history: %s", seqs(d.Chunks))
	}
	d = ask(t, f, quevu.Request{Op: quevu.OpHistoryBefore, Seq: 23, N: 5})
	if seqs(d.Chunks) != "18..22" {
		t.Fatalf("across the archives: %s", seqs(d.Chunks))
	}
	d = ask(t, f, quevu.Request{Op: quevu.OpHistoryBefore, Seq: 1, N: 5})
	if len(d.Chunks) != 0 {
		t.Fatalf("before the first line: %s", seqs(d.Chunks))
	}

	// Walking forwards crosses the archives and then leaves the history,
	// which needs nobody's permission.
	d = ask(t, f, quevu.Request{Op: quevu.OpHistoryAfter, Seq: 18, N: 5})
	if seqs(d.Chunks) != "19..23" {
		t.Fatalf("forwards across the archives: %s", seqs(d.Chunks))
	}
	d = ask(t, f, quevu.Request{Op: quevu.OpHistoryAfter, Seq: 48, N: 5})
	if seqs(d.Chunks) != "49..50" {
		t.Fatalf("end of the history: %s", seqs(d.Chunks))
	}
	d = ask(t, f, quevu.Request{Op: quevu.OpHistoryAfter, Seq: 50, N: 3})
	if seqs(d.Chunks) != "51..53" {
		t.Fatalf("out of the history: %s", seqs(d.Chunks))
	}
}

func TestNoHistoryNoOffer(t *testing.T) {
	scr := New("t", WithMemory(10))
	to(scr).lines("l", 1, 30)
	src := quevu.Join(scr, quevu.Roles{Immediate: holding(11, 30)})
	f := src.Subscribe(0)
	defer f.Close()
	d := ask(t, f, quevu.Request{Op: quevu.OpBefore, Seq: 11, N: 10})
	if len(d.Chunks) != 0 || d.History {
		t.Fatalf("got %s, offer=%v", seqs(d.Chunks), d.History)
	}
	// Asking for a history that is not configured finds nothing.
	d = ask(t, f, quevu.Request{Op: quevu.OpHistoryBefore, Seq: 11, N: 10})
	if len(d.Chunks) != 0 {
		t.Fatalf("got %s", seqs(d.Chunks))
	}
}

func TestOnlyAHistoryBehindTheScreen(t *testing.T) {
	// No immediate provider at all: the screen's memory, then the offer.
	scr := New("t", WithMemory(10))
	to(scr).lines("l", 1, 30)
	src := quevu.Join(scr, quevu.Roles{Historical: holding(1, 30)})
	f := src.Subscribe(0)
	defer f.Close()
	d := ask(t, f, quevu.Request{Op: quevu.OpBefore, Seq: 25, N: 10})
	if seqs(d.Chunks) != "21..24" || !d.History {
		t.Fatalf("got %s, offer=%v", seqs(d.Chunks), d.History)
	}
	d = ask(t, f, quevu.Request{Op: quevu.OpHistoryBefore, Seq: 21, N: 10})
	if seqs(d.Chunks) != "11..20" {
		t.Fatalf("history: %s", seqs(d.Chunks))
	}
}

func TestASearchStaysOutOfTheHistoryUntilAsked(t *testing.T) {
	src, _, old, older := threeLevels()
	// "l30" exists, but only in an archive.
	r, err := quevu.Search(src, quevu.Query{Text: "l30"})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Matches) != 0 || !r.Done || !r.History || r.Examined != 50 || r.Reached != 51 {
		t.Fatalf("%+v", r)
	}
	if old.calls()+older.calls() != 0 {
		t.Fatal("the history was searched without the user asking")
	}
	// The user asks: the same search goes on from where it stopped.
	r, _ = quevu.Search(src, quevu.Query{Text: "l30", From: r.Reached, History: true})
	if got := found(r); len(got) != 1 || got[0] != 30 || r.History {
		t.Fatalf("in the history: %v %+v", got, r)
	}
	// Something recent is found without any of that.
	r, _ = quevu.Search(src, quevu.Query{Text: "l77"})
	if got := found(r); len(got) != 1 || got[0] != 77 || r.History {
		t.Fatalf("recent: %v %+v", got, r)
	}
	// From inside the history, forwards, a search walks out of it.
	r, _ = quevu.Search(src, quevu.Query{Text: "l95", From: 10, Forward: true, History: true})
	if got := found(r); len(got) != 1 || got[0] != 95 {
		t.Fatalf("forwards out of the history: %v %+v", got, r)
	}
	// With the whole history allowed and nothing to find, it really ends.
	r, _ = quevu.Search(src, quevu.Query{Text: "nothing", History: true})
	if len(r.Matches) != 0 || !r.Done || r.History || r.Examined != 100 {
		t.Fatalf("to the very start: %+v", r)
	}
}

func TestASearchWithNoHistoryMakesNoOffer(t *testing.T) {
	src, _, _, _ := joined(10, 100, plain)
	r, _ := quevu.Search(src, quevu.Query{Text: "nothing"})
	if !r.Done || r.History {
		t.Fatalf("%+v", r)
	}
}

func TestAggregateLooksLikeOneProvider(t *testing.T) {
	a := quevu.Aggregate(holding(1, 20), nil, holding(21, 50), holding(51, 60))
	cases := []struct {
		name string
		got  func() ([]quevu.Chunk, error)
		want string
	}{
		{"tail in one part", func() ([]quevu.Chunk, error) { return a.Tail(5) }, "56..60"},
		{"tail across parts", func() ([]quevu.Chunk, error) { return a.Tail(45) }, "16..60"},
		{"tail of everything", func() ([]quevu.Chunk, error) { return a.Tail(1000) }, "1..60"},
		{"before in one part", func() ([]quevu.Chunk, error) { return a.Before(40, 5) }, "35..39"},
		{"before across parts", func() ([]quevu.Chunk, error) { return a.Before(25, 10) }, "15..24"},
		{"before the first", func() ([]quevu.Chunk, error) { return a.Before(1, 10) }, ""},
		{"before past the end", func() ([]quevu.Chunk, error) { return a.Before(999, 3) }, "58..60"},
		{"after in one part", func() ([]quevu.Chunk, error) { return a.After(5, 5) }, "6..10"},
		{"after across parts", func() ([]quevu.Chunk, error) { return a.After(18, 40) }, "19..58"},
		{"after the last", func() ([]quevu.Chunk, error) { return a.After(60, 5) }, ""},
		{"nothing asked", func() ([]quevu.Chunk, error) { return a.Tail(0) }, ""},
	}
	for _, c := range cases {
		got, err := c.got()
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if seqs(got) != c.want {
			t.Fatalf("%s: got %s, want %s", c.name, seqs(got), c.want)
		}
	}
}

func TestAggregateGivesAnOverlapOnce(t *testing.T) {
	// Two archives that share lines 21..30.
	a := quevu.Aggregate(holding(1, 30), holding(21, 50))
	if got, _ := a.Before(40, 30); seqs(got) != "10..39" {
		t.Fatalf("before: %s", seqs(got))
	}
	if got, _ := a.After(15, 25); seqs(got) != "16..40" {
		t.Fatalf("after: %s", seqs(got))
	}
	if got, _ := a.Tail(50); seqs(got) != "1..50" {
		t.Fatalf("tail: %s", seqs(got))
	}
}

// broken is a part of a history that cannot be reached.
type broken struct{}

var errGone = errors.New("archive unreachable")

func (broken) Tail(int) ([]quevu.Chunk, error)           { return nil, errGone }
func (broken) Before(uint64, int) ([]quevu.Chunk, error) { return nil, errGone }
func (broken) After(uint64, int) ([]quevu.Chunk, error)  { return nil, errGone }

func TestAggregateWithAPartOutOfReach(t *testing.T) {
	// One part out of reach does not hide the others.
	a := quevu.Aggregate(holding(1, 20), broken{}, holding(41, 60))
	if got, err := a.Tail(30); err != nil || len(got) != 30 || got[29].Seq != 60 || got[0].Seq != 11 {
		t.Fatalf("tail: %d chunks, %v", len(got), err)
	}
	if got, err := a.After(15, 10); err != nil || len(got) != 10 || got[0].Seq != 16 || got[9].Seq != 45 {
		t.Fatalf("after: %d chunks, %v", len(got), err)
	}
	// With nothing at all to give, the error is not swallowed.
	if _, err := quevu.Aggregate(broken{}).Tail(5); !errors.Is(err, errGone) {
		t.Fatalf("got %v", err)
	}
	// A search over an aggregate is a search over one provider.
	r, _ := quevu.Search(quevu.Aggregate(holding(1, 20), holding(21, 50)), quevu.Query{Text: "l7"})
	if got := found(r); len(got) != 1 || got[0] != 7 || r.Examined != 44 {
		t.Fatalf("search: %v %+v", got, r)
	}
}
