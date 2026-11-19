---
mkskill:
  pos: 10
  in: readme
---

# quevu-panel

The panel of [quevu](https://github.com/pablo-botella/quevu) as a consumer
of a log.

A `Screen` is attached to a log like any other consumer, next to the file
or the database. It is the volatile one: it keeps the last few hundred
chunks in memory, hands new ones to whoever is watching, and stores
nothing.

```go
screen := qpanel.New("web")
web, _ := qlog.New("web",
	qlog.WithConsumer(file),   // persistent
	qlog.WithConsumer(screen), // volatile
)
screen.ShowStats(web) // so the console can show the queues of its log

panel.AddCloser(web)
panel.AddSource(quevu.WithHistory(screen, qtxt.NewReader("web.log")))
```

The log knows none of this. To the log a screen is a consumer that happens
to be fast.