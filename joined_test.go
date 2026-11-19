package qpanel

import (
	"fmt"
	"testing"
	"time"

	"github.com/pablo-botella/quevu"
)

// These tests put a screen next to a store, the way an application does:
// both consume the same log, the screen remembers a little and the store
// everything, and a console reads through the two joined.

// joined writes n lines to a screen that remembers `memory` and to a
// store that keeps all, and returns the source a console would be given.
func joined(memory, n int, line func(i int) string) (quevu.Source, *Screen, *store, *writer) {
	scr := New("t", WithMemory(memory))
	st := &store{}
	w := to(scr, st)
	for i := 1; i <= n; i++ {
		w.put(line(i))
	}
	return quevu.Join(scr, quevu.Roles{Immediate: st}), scr, st, w
}

func plain(i int) string { return fmt.Sprintf("l%d", i) }

func found(r quevu.Result) []uint64 {
	out := make([]uint64, len(r.Matches))
	for i, c := range r.Matches {
		out[i] = c.Seq
	}
	return out
}

func TestHistoryIsSeamless(t *testing.T) {
	src, _, st, _ := joined(10, 100, plain)

	// Inside the short memory the store is never bothered.
	got, _ := src.Before(100, 5)
	if want := []string{"l95", "l96", "l97", "l98", "l99"}; !equal(texts(got), want) || st.calls() != 0 {
		t.Fatalf("got %q, store asked %d times", texts(got), st.calls())
	}
	// Across the edge of the memory (it holds 91..100) both answer.
	got, _ = src.Before(94, 6)
	if want := []string{"l88", "l89", "l90", "l91", "l92", "l93"}; !equal(texts(got), want) {
		t.Fatalf("across: %q", texts(got))
	}
	// Past it, only the store.
	got, _ = src.Before(50, 3)
	if want := []string{"l47", "l48", "l49"}; !equal(texts(got), want) {
		t.Fatalf("stored: %q", texts(got))
	}
	// The very beginning is the end of the way back.
	if got, _ = src.Before(1, 3); len(got) != 0 {
		t.Fatalf("before the first: %q", texts(got))
	}
	// Forwards from a position the memory no longer holds: no gap.
	got, _ = src.After(40, 3)
	if want := []string{"l41", "l42", "l43"}; !equal(texts(got), want) {
		t.Fatalf("after, stored: %q", texts(got))
	}
	got, _ = src.After(95, 3)
	if want := []string{"l96", "l97", "l98"}; !equal(texts(got), want) {
		t.Fatalf("after, recent: %q", texts(got))
	}
	// A tail longer than the memory is completed from the store.
	got, _ = src.Tail(15)
	if len(got) != 15 || string(got[0].Data) != "l86" || string(got[14].Data) != "l100" {
		t.Fatalf("tail: %q", texts(got))
	}
	if src.Name() != "t" {
		t.Fatalf("name %q", src.Name())
	}
}

func TestWithoutProvidersItIsTheScreenItself(t *testing.T) {
	s := New("t")
	if quevu.Join(s, quevu.Roles{}) != quevu.Source(s) {
		t.Fatal("no providers should change nothing")
	}
}

func TestHistoryThroughTheFeed(t *testing.T) {
	src, _, _, w := joined(10, 100, plain)
	f := src.Subscribe(0)
	defer f.Close()

	f.Req <- quevu.Request{Op: quevu.OpFollow, N: 3}
	if d := recv(t, f); d.Op != quevu.OpFollow || !equal(texts(d.Chunks), []string{"l98", "l99", "l100"}) {
		t.Fatalf("follow: %v %q", d.Op, texts(d.Chunks))
	}
	w.put("l101")
	if d := recv(t, f); d.Op != quevu.OpLive || !equal(texts(d.Chunks), []string{"l101"}) {
		t.Fatalf("live: %v %q", d.Op, texts(d.Chunks))
	}
	f.Req <- quevu.Request{Op: quevu.OpPause}
	if d := recv(t, f); d.Op != quevu.OpPause {
		t.Fatalf("pause: %v", d.Op)
	}
	// Further back than the screen remembers: the feed reaches the store.
	f.Req <- quevu.Request{Op: quevu.OpBefore, Seq: 20, N: 2}
	if d := recv(t, f); d.Op != quevu.OpBefore || !equal(texts(d.Chunks), []string{"l18", "l19"}) {
		t.Fatalf("before: %v %q", d.Op, texts(d.Chunks))
	}
	f.Req <- quevu.Request{Op: quevu.OpAfter, Seq: 19, N: 2}
	if d := recv(t, f); d.Op != quevu.OpAfter || !equal(texts(d.Chunks), []string{"l20", "l21"}) {
		t.Fatalf("after: %v %q", d.Op, texts(d.Chunks))
	}
}

func TestJoinedFeedSurvivesASilentConsole(t *testing.T) {
	src, _, _, w := joined(10, 100, plain)
	f := src.Subscribe(4)
	f.Req <- quevu.Request{Op: quevu.OpFollow}
	recv(t, f)
	// Nobody reads while output pours in and requests pile up.
	w.lines("x", 0, 1999)
	for i := 0; i < 10; i++ {
		f.Req <- quevu.Request{Op: quevu.OpPause}
	}
	// Reading again finds a feed that still works.
	f.Req <- quevu.Request{Op: quevu.OpFollow, N: 1}
	for {
		d := recv(t, f)
		if d.Op == quevu.OpFollow {
			if !equal(texts(d.Chunks), []string{"x1999"}) {
				t.Fatalf("resume: %q", texts(d.Chunks))
			}
			break
		}
	}
	f.Close()
	for range f.Data {
	}
}

// searchable holds 5000 lines; every 1000th is an error, colored the way
// a terminal program would color it. The screen remembers the last 100.
func searchable() (quevu.Source, *store) {
	src, _, st, _ := joined(100, 5000, func(i int) string {
		if i%1000 == 0 {
			return fmt.Sprintf("line %d \x1b[31mERROR\x1b[0m disk \x1b[1mfull\x1b[0m", i)
		}
		return fmt.Sprintf("line %d ok", i)
	})
	return src, st
}

func TestSearchWalksBackFromTheEnd(t *testing.T) {
	src, _ := searchable()
	r, err := quevu.Search(src, quevu.Query{Text: "error"})
	if err != nil {
		t.Fatal(err)
	}
	// The newest match, found case-blind, in the short memory.
	if got := found(r); len(got) != 1 || got[0] != 5000 {
		t.Fatalf("found %v", got)
	}
	// The next step starts where this one stopped, and leaves the screen
	// for the store without anybody having to say so.
	r, _ = quevu.Search(src, quevu.Query{Text: "error", From: r.Reached})
	if got := found(r); len(got) != 1 || got[0] != 4000 {
		t.Fatalf("second step found %v", got)
	}
	if r.Reached != 4000 || r.Examined != 1000 || r.Done {
		t.Fatalf("second step: %+v", r)
	}
}

func TestSearchSeesThroughColors(t *testing.T) {
	src, _ := searchable()
	// The phrase crosses three changes of color in the stored bytes.
	r, _ := quevu.Search(src, quevu.Query{Text: "ERROR disk full", From: 3500})
	if got := found(r); len(got) != 1 || got[0] != 3000 {
		t.Fatalf("found %v", got)
	}
	// And an escape code is never what gets matched.
	r, _ = quevu.Search(src, quevu.Query{Text: "[31m", Budget: 6000})
	if len(r.Matches) != 0 || !r.Done {
		t.Fatalf("matched an escape sequence: %+v", r)
	}
}

func TestSearchStopsAtItsBudget(t *testing.T) {
	src, _ := searchable()
	r, _ := quevu.Search(src, quevu.Query{Text: "no such thing", Budget: 1500})
	if len(r.Matches) != 0 || r.Done {
		t.Fatalf("%+v", r)
	}
	// It looked at exactly what it was allowed to, and says how far back
	// that was, so the next step can go on from there.
	if r.Examined != 1500 || r.Reached != 3501 || r.ReachedTime == 0 {
		t.Fatalf("examined %d, reached %d", r.Examined, r.Reached)
	}
	r, _ = quevu.Search(src, quevu.Query{Text: "no such thing", From: r.Reached, Budget: 10000})
	if len(r.Matches) != 0 || !r.Done || r.Reached != 1 || r.Examined != 3500 {
		t.Fatalf("to the start: %+v", r)
	}
}

func TestSearchForwards(t *testing.T) {
	src, _ := searchable()
	r, _ := quevu.Search(src, quevu.Query{Text: "error", From: 1000, Forward: true})
	if got := found(r); len(got) != 1 || got[0] != 2000 {
		t.Fatalf("found %v", got)
	}
	r, _ = quevu.Search(src, quevu.Query{Text: "error", From: 5000, Forward: true})
	if len(r.Matches) != 0 || !r.Done {
		t.Fatalf("past the end: %+v", r)
	}
	// Several at once come in order.
	r, _ = quevu.Search(src, quevu.Query{Text: "error", Forward: true, Max: 3})
	if got := found(r); len(got) != 3 || got[0] != 1000 || got[2] != 3000 {
		t.Fatalf("found %v", got)
	}
	r, _ = quevu.Search(src, quevu.Query{Text: "error", Max: 3})
	if got := found(r); len(got) != 3 || got[0] != 3000 || got[2] != 5000 {
		t.Fatalf("backwards, found %v", got)
	}
}

func TestSearchWithinATimeRange(t *testing.T) {
	// 300 lines, one per minute ending now; the word is on every tenth.
	scr := New("t", WithMemory(10))
	st := &store{}
	w := to(scr, st)
	start := time.Now().Add(-299 * time.Minute).Truncate(time.Second)
	n := 0
	w.now = func() time.Time { n++; return start.Add(time.Duration(n-1) * time.Minute) }
	for i := 1; i <= 300; i++ {
		if i%10 == 0 {
			w.put(fmt.Sprintf("line %d alarm", i))
		} else {
			w.put(fmt.Sprintf("line %d", i))
		}
	}
	at := func(line int) time.Time { return start.Add(time.Duration(line-1) * time.Minute) }
	src := quevu.Join(scr, quevu.Roles{Immediate: st})

	// Between lines 95 and 125: the alarms at 100, 110 and 120.
	q := quevu.Query{Text: "alarm", Since: at(95), Until: at(125), Max: 10}
	r, err := quevu.Search(src, q)
	if err != nil {
		t.Fatal(err)
	}
	if got := found(r); len(got) != 3 || got[0] != 100 || got[2] != 120 {
		t.Fatalf("found %v", got)
	}
	// It stopped as soon as it walked out of the range, and says why:
	// there is no point in going further back, or into any history.
	if !r.Done || !r.OutOfRange || r.Reached != 94 || r.History {
		t.Fatalf("%+v", r)
	}
	// One at a time, newest first, like the console asks.
	q.Max = 1
	r, _ = quevu.Search(src, q)
	if got := found(r); len(got) != 1 || got[0] != 120 {
		t.Fatalf("first step found %v", got)
	}
	q.From = r.Reached
	r, _ = quevu.Search(src, q)
	if got := found(r); len(got) != 1 || got[0] != 110 {
		t.Fatalf("second step found %v", got)
	}
	// Forwards the same range is entered from below and left from above.
	r, _ = quevu.Search(src, quevu.Query{Text: "alarm", Since: at(95), Until: at(125), Max: 10, Forward: true})
	if got := found(r); len(got) != 3 || got[0] != 100 || !r.OutOfRange || r.Reached != 126 {
		t.Fatalf("forwards: %v %+v", got, r)
	}
	// Only a lower bound, only an upper bound.
	r, _ = quevu.Search(src, quevu.Query{Text: "alarm", Since: at(281), Max: 10})
	if got := found(r); len(got) != 2 || got[0] != 290 || got[1] != 300 {
		t.Fatalf("since only: %v", got)
	}
	r, _ = quevu.Search(src, quevu.Query{Text: "alarm", Until: at(25), Max: 10})
	if got := found(r); len(got) != 2 || got[0] != 10 || got[1] != 20 || r.OutOfRange {
		t.Fatalf("until only: %v %+v", got, r)
	}
	// A range with nothing in it.
	r, _ = quevu.Search(src, quevu.Query{Text: "alarm", Since: at(101), Until: at(109)})
	if len(r.Matches) != 0 || !r.Done {
		t.Fatalf("empty range: %+v", r)
	}
}

func TestATimeRangeEndsTheOfferOfHistory(t *testing.T) {
	scr := New("t", WithMemory(10))
	recent, old := &store{}, &store{}
	start := time.Now().Add(-99 * time.Minute)
	for i := 1; i <= 100; i++ {
		c := []quevu.Chunk{{Seq: uint64(i), Time: start.Add(time.Duration(i-1) * time.Minute), Data: fmt.Appendf(nil, "line %d", i)}}
		scr.Write(c)
		if i > 50 {
			recent.Write(c)
		} else {
			old.Write(c)
		}
	}
	src := quevu.Join(scr, quevu.Roles{Immediate: recent, Historical: old})
	// The range ends inside recent output: the history cannot help, so
	// it is not offered.
	r, _ := quevu.Search(src, quevu.Query{Text: "nothing", Since: start.Add(70 * time.Minute)})
	if !r.Done || !r.OutOfRange || r.History || old.calls() != 0 {
		t.Fatalf("%+v, history asked %d times", r, old.calls())
	}
	// The range reaches further back than recent output: it is.
	r, _ = quevu.Search(src, quevu.Query{Text: "nothing", Since: start.Add(20 * time.Minute)})
	if !r.Done || r.OutOfRange || !r.History {
		t.Fatalf("%+v", r)
	}
	r, _ = quevu.Search(src, quevu.Query{Text: "line 30", Since: start.Add(20 * time.Minute), From: r.Reached, History: true})
	if got := found(r); len(got) != 1 || got[0] != 30 {
		t.Fatalf("in the history, in range: %v", got)
	}
}

func TestSearchForNothingFindsNothing(t *testing.T) {
	src, _ := searchable()
	r, err := quevu.Search(src, quevu.Query{})
	if err != nil || len(r.Matches) != 0 || r.Examined != 0 {
		t.Fatalf("%+v %v", r, err)
	}
}

func TestPlainText(t *testing.T) {
	cases := map[string]string{
		"plain":                          "plain",
		"\x1b[31mred\x1b[0m":             "red",
		"a\x1b[38;2;1;2;3mb\x1b[1;4mc":   "abc",
		"a\x1bMb":                        "ab",
		"cut \x1b[3":                     "cut ",
		"cut \x1b":                       "cut ",
		"tab\tand \xff bytes stay":       "tab\tand \xff bytes stay",
		"\x1b[2K\x1b[10;5Hmoved":         "moved",
		"áñ \x1b[32m€\x1b[0m \U0001f600": "áñ € \U0001f600",
	}
	for in, want := range cases {
		if got := string(quevu.PlainText(nil, []byte(in))); got != want {
			t.Errorf("%q: got %q, want %q", in, got, want)
		}
	}
}

// aged holds 300 lines, one per minute ending now. The screen remembers
// the last 10.
func aged() (*Screen, *store) {
	scr := New("t", WithMemory(10))
	st := &store{}
	w := to(scr, st)
	start := time.Now().Add(-299 * time.Minute)
	n := 0
	w.now = func() time.Time {
		n++
		return start.Add(time.Duration(n-1) * time.Minute)
	}
	for i := 1; i <= 300; i++ {
		w.put(fmt.Sprintf("line %d", i))
	}
	return scr, st
}

func TestProviderCapsWhatOneRequestReturns(t *testing.T) {
	_, st := aged()
	p := quevu.Limit(st, quevu.Limits{Page: 5})
	got, _ := p.Before(200, 50)
	if len(got) != 5 || got[0].Seq != 195 || got[4].Seq != 199 {
		t.Fatalf("got %d chunks", len(got))
	}
	if got, _ := p.Tail(50); len(got) != 5 || got[4].Seq != 300 {
		t.Fatalf("tail: %d", len(got))
	}
	if got, _ := p.After(10, 50); len(got) != 5 || got[0].Seq != 11 {
		t.Fatalf("after: %d", len(got))
	}
}

func TestProviderHidesWhatIsTooOld(t *testing.T) {
	scr, st := aged()
	// Line 300 is from now, line 270 from half an hour ago.
	p := quevu.Limit(st, quevu.Limits{MaxAge: 30*time.Minute + 30*time.Second})
	got, _ := p.Before(280, 100)
	if len(got) == 0 || got[0].Seq != 270 || got[len(got)-1].Seq != 279 {
		t.Fatalf("got %d chunks: %v", len(got), found(quevu.Result{Matches: got}))
	}
	// Past the horizon the log simply is not there.
	if got, _ := p.Before(270, 100); len(got) != 0 {
		t.Fatalf("got %d chunks from beyond the horizon", len(got))
	}
	// A console joined to it walks back to the horizon and stops.
	src := quevu.Join(scr, quevu.Roles{Immediate: p})
	if got, _ := src.Before(275, 100); len(got) != 5 || got[0].Seq != 270 {
		t.Fatalf("joined: %d chunks", len(got))
	}
	if got, _ := src.Before(270, 100); len(got) != 0 {
		t.Fatalf("joined, beyond: %d chunks", len(got))
	}
}

func TestProviderSetsTheTermsOfASearch(t *testing.T) {
	scr, st := aged()

	// It may cap its part of the step, whatever the query asks: the ten
	// chunks the screen remembers are the screen's to search, the next
	// fifty are all the store is willing to look at.
	src := quevu.Join(scr, quevu.Roles{Immediate: quevu.Limit(st, quevu.Limits{SearchBudget: 50})})
	r, _ := quevu.Search(src, quevu.Query{Text: "line 7 ", Budget: 1_000_000})
	if r.Examined != 60 || r.Reached != 241 || r.Done || len(r.Matches) != 0 {
		t.Fatalf("capped: %+v", r)
	}

	// It may refuse. What the screen remembers is still searched.
	src = quevu.Join(scr, quevu.Roles{Immediate: quevu.Limit(st, quevu.Limits{NoSearch: true})})
	r, _ = quevu.Search(src, quevu.Query{Text: "line 295"})
	if len(r.Matches) != 1 || r.Matches[0].Seq != 295 {
		t.Fatalf("in memory: %+v", r)
	}
	r, _ = quevu.Search(src, quevu.Query{Text: "line 7 "})
	if len(r.Matches) != 0 || !r.Done || r.Examined != 10 {
		t.Fatalf("refused: %+v", r)
	}
	// Reading is another matter: it was not refused.
	if got, _ := src.Before(50, 3); len(got) != 3 {
		t.Fatalf("read: %d", len(got))
	}

	// A search respects the horizon as well.
	p := quevu.Limit(st, quevu.Limits{MaxAge: 30*time.Minute + 30*time.Second})
	r, _ = quevu.Search(p, quevu.Query{Text: "line 26"})
	if len(r.Matches) != 0 || !r.Done || r.Reached != 270 {
		t.Fatalf("beyond the horizon: %+v", r)
	}
}
