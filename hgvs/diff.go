// Copyright (C) The Lightning Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package hgvs

import (
	"fmt"
	"strings"
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
	case len(v.New) == 1 && v.New == v.Ref:
		return fmt.Sprintf("%d=", v.Position)
	case v.New == v.Ref:
		return fmt.Sprintf("%d_%d=", v.Position, v.Position+len(v.Ref)-1)
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
		if len(v.Ref) == 2 && len(v.New) == 2 {
			v1 := v
			v1.Ref = v1.Ref[:1]
			v1.New = v1.New[:1]
			v.Ref = v.Ref[1:]
			v.New = v.New[1:]
			v.Position++
			v.Left = v1.Ref
			pos++
			variants = append(variants, v1)
		}
		pos += len(v.Ref)
		variants = append(variants, v)
		left = ""
	}
	return variants, timedOut
}

func cleanup(in []diffmatchpatch.Diff) (out []diffmatchpatch.Diff) {
	out = make([]diffmatchpatch.Diff, 0, len(in))
	for i := 0; i < len(in); i++ {
		d := in[i]
		// Merge consecutive entries of same type (e.g.,
		// "insert A; insert B")
		for i < len(in)-1 && in[i].Type == in[i+1].Type {
			d.Text += in[i+1].Text
			i++
		}
		out = append(out, d)
	}
	in, out = out, make([]diffmatchpatch.Diff, 0, len(in))
	for i := 0; i < len(in); i++ {
		d := in[i]
		// when diffmatchpatch says [=yyyyXXXX, delX, =zzz],
		// we really want [=yyyy, delX, =XXXXzzz] (ditto for
		// ins instead of del)
		if i < len(in)-2 &&
			d.Type == diffmatchpatch.DiffEqual &&
			in[i+1].Type != diffmatchpatch.DiffEqual &&
			in[i+2].Type == diffmatchpatch.DiffEqual &&
			len(in[i+1].Text) <= len(d.Text) {
			for cut := 0; cut < len(d.Text)-len(in[i+1].Text); cut++ {
				if d.Text[cut:] == d.Text[cut+len(in[i+1].Text):]+in[i+1].Text {
					in[i+2].Text = d.Text[cut+len(in[i+1].Text):] + in[i+1].Text + in[i+2].Text
					in[i+1].Text = d.Text[cut : cut+len(in[i+1].Text)]
					d.Text = d.Text[:cut]
					break
				}
			}
		}
		// diffmatchpatch solves diff("AAX","XTX") with
		// [delAA,=X,insTX] but we prefer to spell it
		// [delAA,insXT,=X].
		//
		// So, when we see a [del,=,ins] sequence where the
		// "=" part is a suffix of the "ins" part -- e.g.,
		// [delAAA,=CGG,insTTTCGG] -- we rearrange it to the
		// equivalent spelling [delAAA,insCGGTTT,=CGG].
		if i < len(in)-2 &&
			d.Type == diffmatchpatch.DiffDelete &&
			in[i+1].Type == diffmatchpatch.DiffEqual &&
			in[i+2].Type == diffmatchpatch.DiffInsert &&
			strings.HasSuffix(in[i+2].Text, in[i+1].Text) {
			eq, ins := in[i+1], in[i+2]
			ins.Text = eq.Text + ins.Text[:len(ins.Text)-len(eq.Text)]
			in[i+1] = ins
			in[i+2] = eq
		}
		// diffmatchpatch solves diff("AXX","XXX") with
		// [delA,=XX,insX] but we prefer to spell it
		// [delA,insX,=XX].
		//
		// So, when we see a [del,=,ins] sequence that has the
		// same effect after swapping the "=" and "ins" parts,
		// we swap them.
		if i < len(in)-2 &&
			d.Type == diffmatchpatch.DiffDelete &&
			in[i+1].Type == diffmatchpatch.DiffEqual &&
			in[i+2].Type == diffmatchpatch.DiffInsert &&
			in[i+1].Text+in[i+2].Text == in[i+2].Text+in[i+1].Text {
			in[i+2], in[i+1] = in[i+1], in[i+2]
		}
		// Likewise, diffmatchpatch solves
		// diff("XXXA","XXAA") with [delX,=XXA,insA], we
		// prefer [=XX,delX,insA,=A]
		if i < len(in)-2 &&
			d.Type == diffmatchpatch.DiffDelete &&
			in[i+1].Type == diffmatchpatch.DiffEqual &&
			in[i+2].Type == diffmatchpatch.DiffInsert {
			redo := false
			for x := len(d.Text); x <= len(in[i+1].Text)-len(in[i+2].Text); x++ {
				// d  in[i+1]  in[i+2]
				// x  xxx aaa  a
				//       ^
				// x  xx
				//    xxx
				//        aaa
				//         aa  a
				if d.Text+in[i+1].Text[:x-len(d.Text)] == in[i+1].Text[:x] &&
					in[i+1].Text[x:] == in[i+1].Text[x+len(in[i+2].Text):]+in[i+2].Text {
					out = append(out, diffmatchpatch.Diff{diffmatchpatch.DiffEqual, d.Text + in[i+1].Text[:x-len(d.Text)]})
					in[i], in[i+1], in[i+2] = diffmatchpatch.Diff{diffmatchpatch.DiffDelete, in[i+1].Text[x-len(d.Text) : x]},
						diffmatchpatch.Diff{diffmatchpatch.DiffInsert, in[i+1].Text[x : x+len(in[i+2].Text)]},
						diffmatchpatch.Diff{diffmatchpatch.DiffEqual, in[i+1].Text[x+len(in[i+2].Text):] + in[i+2].Text}
					redo = true
					break
				}
			}
			if redo {
				i--
				continue
			}
		}
		// when diffmatchpatch says [delAAA, insXAY] and
		// len(X)==1, we prefer to treat the A>X as a snp.
		if i < len(in)-1 &&
			d.Type == diffmatchpatch.DiffDelete &&
			in[i+1].Type == diffmatchpatch.DiffInsert &&
			len(d.Text) >= 2 &&
			len(in[i+1].Text) >= 2 &&
			d.Text[1] == in[i+1].Text[1] {
			eqend := 2
			for ; eqend < len(d.Text) && eqend < len(in[i+1].Text) && d.Text[eqend] == in[i+1].Text[eqend]; eqend++ {
			}
			out = append(out,
				diffmatchpatch.Diff{diffmatchpatch.DiffDelete, d.Text[:1]},
				diffmatchpatch.Diff{diffmatchpatch.DiffInsert, in[i+1].Text[:1]},
				diffmatchpatch.Diff{diffmatchpatch.DiffEqual, d.Text[1:eqend]})
			in[i].Text, in[i+1].Text = in[i].Text[eqend:], in[i+1].Text[eqend:]
			i--
			continue
		}
		// when diffmatchpatch says [delAAA, insXaY] and
		// len(Y)==1, we prefer to treat the A>Y as a snp.
		if i < len(in)-1 &&
			d.Type == diffmatchpatch.DiffDelete &&
			in[i+1].Type == diffmatchpatch.DiffInsert &&
			len(d.Text) >= 2 &&
			len(in[i+1].Text) >= 2 &&
			d.Text[len(d.Text)-2] == in[i+1].Text[len(in[i+1].Text)-2] {
			// eqstart will be the number of equal chars
			// before the terminal snp, plus 1 for the snp
			// itself. Example, for [delAAAA, insTTAAG],
			// eqstart will be 3.
			eqstart := 2
			for ; eqstart < len(d.Text) && eqstart < len(in[i+1].Text) && d.Text[len(d.Text)-eqstart] == in[i+1].Text[len(in[i+1].Text)-eqstart]; eqstart++ {
			}
			eqstart--
			out = append(out,
				diffmatchpatch.Diff{diffmatchpatch.DiffDelete, d.Text[:len(d.Text)-eqstart]},
				diffmatchpatch.Diff{diffmatchpatch.DiffInsert, in[i+1].Text[:len(in[i+1].Text)-eqstart]},
				diffmatchpatch.Diff{diffmatchpatch.DiffEqual, d.Text[len(d.Text)-eqstart : len(d.Text)-1]},
				diffmatchpatch.Diff{diffmatchpatch.DiffDelete, d.Text[len(d.Text)-1:]},
				diffmatchpatch.Diff{diffmatchpatch.DiffInsert, in[i+1].Text[len(in[i+1].Text)-1:]})
			i++
			continue
		}
		// [=AB,insCB,=D] => [=A,insBC,=BD]
		// and
		// [=AB,delCB,=D] => [=A,delBC,=BD]
		if i < len(in)-2 &&
			d.Type == diffmatchpatch.DiffEqual &&
			in[i+1].Type != diffmatchpatch.DiffEqual &&
			in[i+2].Type == diffmatchpatch.DiffEqual &&
			len(d.Text) > 0 && len(in[i+1].Text) > 0 &&
			!(i+3 < len(in) &&
				// Except: leave deletion alone if an
				// upcoming insertion will be moved up
				// against it: e.g., for
				// [=AB,delCB,=D,insED] we want
				// [=AB,delCB,=D,insED] for now, so it
				// can become [=AB,delCB,insDE,=D] on
				// the next iteration.
				in[i+1].Type == diffmatchpatch.DiffDelete &&
				in[i+3].Type == diffmatchpatch.DiffInsert &&
				strings.HasSuffix(in[i+3].Text, in[i+2].Text)) {
			if i+3 < len(in) && in[i+1].Type == in[i+3].Type && strings.HasSuffix(in[i+3].Text, in[i+2].Text) {
				// [=AB,delC,=E,delDBE] => [=AB,delCEDB,=E,=]
				in[i+1], in[i+2], in[i+3] = diffmatchpatch.Diff{in[i+1].Type, in[i+1].Text + in[i+2].Text + in[i+3].Text[:len(in[i+3].Text)-len(in[i+2].Text)]},
					diffmatchpatch.Diff{diffmatchpatch.DiffEqual, in[i+2].Text},
					diffmatchpatch.Diff{diffmatchpatch.DiffEqual, ""}
			}
			// Find x, length of common suffix B
			x := 1
			for ; x <= len(d.Text) && x <= len(in[i+1].Text); x++ {
				if d.Text[len(d.Text)-x] != in[i+1].Text[len(in[i+1].Text)-x] {
					break
				}
			}
			x--
			d.Text, in[i+1].Text, in[i+2].Text =
				d.Text[:len(d.Text)-x],
				d.Text[len(d.Text)-x:]+
					in[i+1].Text[:len(in[i+1].Text)-x],
				in[i+1].Text[len(in[i+1].Text)-x:]+in[i+2].Text
		}
		// [=X,delAX] => [delXA,=X]
		if i < len(in)-1 &&
			d.Type == diffmatchpatch.DiffEqual &&
			in[i+1].Type == diffmatchpatch.DiffDelete && false {
		}
		out = append(out, d)
	}
	in, out = out, make([]diffmatchpatch.Diff, 0, len(in))
	for _, d := range in {
		if len(d.Text) > 0 {
			out = append(out, d)
		}
	}
	// for i := 0; i < len(out)-1; i++ {
	// 	if out[i].Type == diffmatchpatch.DiffDelete && len(out[i].Text) == 2 &&
	// 		out[i+1].Type == diffmatchpatch.DiffInsert && len(out[i+1].Text) == 2 {
	// 		out = append(out, diffmatchpatch.Diff{}, diffmatchpatch.Diff{})
	// 		copy(out[i+4:], out[i+2:])
	// 		out[i+2] = diffmatchpatch.Diff{diffmatchpatch.DiffDelete, out[i].Text[1:]}
	// 		out[i+3] = diffmatchpatch.Diff{diffmatchpatch.DiffInsert, out[i+1].Text[1:]}
	// 		out[i].Text = out[i].Text[:1]
	// 		out[i+1].Text = out[i+1].Text[:1]
	// 	}
	// }
	return
}

func Less(a, b Variant) bool {
	if a.Position != b.Position {
		return a.Position < b.Position
	} else if a.New != b.New {
		return a.New < b.New
	} else {
		return a.Ref < b.Ref
	}
}
