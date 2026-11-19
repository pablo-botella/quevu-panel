# quevu-panel - agent notes

Go package `github.com/pablo-botella/quevu-panel`, imported as `qpanel`:
the panel as a consumer. One public type, `qpanel.Screen`, which
implements `quevu.Consumer`, `quevu.Source` and `quevu.StatSource`. It
depends only on `github.com/pablo-botella/quevu` (the contracts): it does
not import the log, and the log does not import it.

## What a screen is

To the log, a consumer: it takes batches and never makes the log wait.

To the console that shows it, two things at once:

- **The live output**, through a feed that is never waited for. A console
  that falls behind is paused; nothing upstream notices.
- **The immediate provider**: what was written a moment ago, given back by
  position without touching any store.

What lies further back is not the screen's business. The providers the
application attaches with `quevu.Join` take over where the short memory
ends: the immediate one by itself, the historical one only when the user
asks for it.

```go
screen := qpanel.New("web", qpanel.WithMemory(500)) // 500 is the default
```

`WithMemory(0)` keeps nothing: the console then gets live output only.

## On a server with no desktop

The screen is the part of quevu that only makes sense when somebody can be
watching. Where nobody can, do not create it: the log has one consumer
less and the application is otherwise the same. Create it when there is a
window, or an endpoint a controller may connect to.

## The queues of the log

A console shows, next to its state, the queues of its log: one that keeps
growing is how one sees that a consumer is too slow. Those numbers belong
to the log, so the screen is only told where to read them from and passes
them on:

```go
screen.ShowStats(theLog)
```

## Closing

`Close` ends the feeds. What the screen remembers stays readable, so a
window still open shows the last words of a log that has closed. It is
safe to call more than once, and it is: the log closes its consumers, and
the panel closes its sources.

## Rules and gotchas

- **One screen per log, one tab per screen.** Its name is the title of the
  tab.
- **A screen stores nothing.** When the application ends, what it held is
  gone. For output that must be kept, the log needs a persistent consumer
  as well; for it to be read back, that consumer needs a provider.
- **`Before` returning nothing means the short memory ends there**, not
  that the log does.
- **Do not hold a feed without reading it** unless you mean to: its buffer
  fills and the screen pauses it. That is by design, and harmless.
- **Internals for changes:** one mutex guards the memory and the set of
  feeds, which is what makes `OpFollow` exact: the last chunks are taken
  and the feed switched to live output in the same step, so nothing falls
  between the two.
