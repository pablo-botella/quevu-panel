---
mkskill:
  pos: 10
  in: readme
---

# quevu-panel

The panel of [quevu](https://github.com/pablo-botella/quevu-kernel) as a writer
of a log.

A `Screen` is attached to a log like any other writer, next to the file
or the database. It is the volatile one: it keeps the last few hundred
chunks in memory, hands new ones to whoever is watching, and stores
nothing.

```go
screen := qpanel.New("web")
web, _ := qhub.New("web",
	qhub.WithWriter(file),   // persistent
	qhub.WithWriterOptions(screen, qhub.Options{OnFull: qhub.Skip}), // volatile: skipped at its limit
)
screen.ShowStats(web) // so the console can show the queues of its log

panel.AddCloser(web)
panel.AddSource(quevu.Join(screen, quevu.Roles{Immediate: qtxt.NewReader("web.log")}))
```

The hub knows none of this. To the hub a screen is one more writer,
connected so that it is skipped at its limit instead of waited for.