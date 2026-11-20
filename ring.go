package qpanel

import (
	"sort"

	quevu "github.com/pablo-botella/quevu-kernel"
)

// ring is the short memory of a screen: the most recent chunks, kept so a
// console can step back a few screens without a stored-data provider.
// Positions inside it are consecutive.
type ring struct {
	buf   []quevu.Chunk
	start int
	n     int
}

func newRing(capacity int) *ring {
	if capacity < 0 {
		capacity = 0
	}
	return &ring{buf: make([]quevu.Chunk, capacity)}
}

func (r *ring) add(c quevu.Chunk) {
	if len(r.buf) == 0 {
		return
	}
	if r.n < len(r.buf) {
		r.buf[(r.start+r.n)%len(r.buf)] = c
		r.n++
		return
	}
	r.buf[r.start] = c
	r.start = (r.start + 1) % len(r.buf)
}

func (r *ring) at(i int) quevu.Chunk {
	return r.buf[(r.start+i)%len(r.buf)]
}

// slice copies the chunks in [i, j).
func (r *ring) slice(i, j int) []quevu.Chunk {
	if i < 0 {
		i = 0
	}
	if j > r.n {
		j = r.n
	}
	if i >= j {
		return nil
	}
	out := make([]quevu.Chunk, 0, j-i)
	for k := i; k < j; k++ {
		out = append(out, r.at(k))
	}
	return out
}

func (r *ring) tail(n int) []quevu.Chunk {
	if n <= 0 {
		return nil
	}
	return r.slice(r.n-n, r.n)
}

// before returns up to n chunks with Seq < seq.
func (r *ring) before(seq uint64, n int) []quevu.Chunk {
	if n <= 0 {
		return nil
	}
	idx := sort.Search(r.n, func(i int) bool { return r.at(i).Seq >= seq })
	return r.slice(idx-n, idx)
}

// after returns up to n chunks with Seq > seq.
func (r *ring) after(seq uint64, n int) []quevu.Chunk {
	if n <= 0 {
		return nil
	}
	idx := sort.Search(r.n, func(i int) bool { return r.at(i).Seq > seq })
	return r.slice(idx, idx+n)
}
