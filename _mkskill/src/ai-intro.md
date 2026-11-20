---
mkskill:
  pos: 5
  in: ai*
---

# quevu-panel - agent notes

Go package `github.com/pablo-botella/quevu-panel`, imported as `qpanel`:
the panel as a consumer. One public type, `qpanel.Screen`, which
implements `quevu.Consumer`, `quevu.Source` and `quevu.StatSource`. It
depends only on `github.com/pablo-botella/quevu-kernel` (the contracts): it does
not import the log, and the log does not import it.