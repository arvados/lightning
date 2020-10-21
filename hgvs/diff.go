package hgvs

import (
	"fmt"
	"time"

	"github.com/sergi/go-diff/diffmatchpatch"
)

type Variant struct {
	Position int
	Ref      string
	New      string
	Left     string // base preceding an indel, if Ref or New is empty
}

func (v *Variant) String() string {
	switch {
	case len(v.New) == 0 && len(v.Ref) == 0:
		return fmt.Sprintf("%d=", v.Position)
	case len(v.New) == 0 && len(v.Ref) == 1:
		return fmt.Sprintf("%ddel", v.Position)
	case len(v.New) == 0:
		return fmt.Sprintf("%d_%ddel", v.Position, v.Position+len(v.Ref)-1)
	case len(v.Ref) == 1 && len(v.New) == 1:
		return fmt.Sprintf("%d%s>%s", v.Position, v.Ref, v.New)
	case len(v.Ref) == 0:
		return fmt.Sprintf("%d_%dins%s", v.Position-1, v.Position, v.New)
	case len(v.Ref) == 1 && len(v.New) > 0:
		return fmt.Sprintf("%ddelins%s", v.Position, v.New)
	default:
		return fmt.Sprintf("%d_%ddelins%s", v.Position, v.Position+len(v.Ref)-1, v.New)
	}
}

// PadLeft returns a Variant that is equivalent to v but (if possible)
// uses the stashed preceding base (the Left field) to avoid having a
// non-empty Ref or New part, even for an insertion or deletion.
//
// For example, if v is {Position: 45, Ref: "", New: "A"}, PadLeft
// might return {Position: 44, Ref: "T", New: "TA"}.
func (v *Variant) PadLeft() Variant {
	if len(v.Ref) == 0 || len(v.New) == 0 {
		return Variant{
			Position: v.Position - len(v.Left),
			Ref:      v.Left + v.Ref,
			New:      v.Left + v.New,
		}
	} else {
		return *v
	}
}

func Diff(a, b string, timeout time.Duration) ([]Variant, bool) {
	dmp := diffmatchpatch.New()
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	diffs := dmp.DiffBisect(a, b, deadline)
	timedOut := false
	if timeout > 0 && time.Now().After(deadline) {
		timedOut = true
	}
	diffs = cleanup(dmp.DiffCleanupEfficiency(diffs))
	pos := 1
	var variants []Variant
	for i := 0; i < len(diffs); {
		left := "" // last char before an insertion or deletion
		for ; i < len(diffs) && diffs[i].Type == diffmatchpatch.DiffEqual; i++ {
			pos += len(diffs[i].Text)
			if tlen := len(diffs[i].Text); tlen > 0 {
				left = diffs[i].Text[tlen-1:]
			}
		}
		if i >= len(diffs) {
			break
		}
		v := Variant{Position: pos, Left: left}
		for ; i < len(diffs) && diffs[i].Type != diffmatchpatch.DiffEqual; i++ {
			if diffs[i].Type == diffmatchpatch.DiffDelete {
				v.Ref += diffs[i].Text
			} else {
				v.New += diffs[i].Text
			}
		}
		pos += len(v.Ref)
		variants = append(variants, v)
		left = ""
	}
	return variants, timedOut
}

func cleanup(in []diffmatchpatch.Diff) (out []diffmatchpatch.Diff) {
	for i := 0; i < len(in); i++ {
		d := in[i]
		for i < len(in)-1 && in[i].Type == in[i+1].Type {
			d.Text += in[i+1].Text
			i++
		}
		out = append(out, d)
	}
	return
}
