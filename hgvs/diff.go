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
	for i := 0; i < len(diffs); i++ {
		switch diffs[i].Type {
		case diffmatchpatch.DiffEqual:
			pos += len(diffs[i].Text)
		case diffmatchpatch.DiffDelete:
			if i+1 < len(diffs) && diffs[i+1].Type == diffmatchpatch.DiffInsert {
				// deletion followed by insertion
				variants = append(variants, Variant{Position: pos, Ref: diffs[i].Text, New: diffs[i+1].Text})
				pos += len(diffs[i].Text)
				i++
			} else {
				variants = append(variants, Variant{Position: pos, Ref: diffs[i].Text})
				pos += len(diffs[i].Text)
			}
		case diffmatchpatch.DiffInsert:
			if i+1 < len(diffs) && diffs[i+1].Type == diffmatchpatch.DiffDelete {
				// insertion followed by deletion
				variants = append(variants, Variant{Position: pos, Ref: diffs[i+1].Text, New: diffs[i].Text})
				pos += len(diffs[i+1].Text)
				i++
			} else {
				variants = append(variants, Variant{Position: pos, New: diffs[i].Text})
			}
		}
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
