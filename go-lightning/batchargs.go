// Copyright (C) The Lightning Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package lightning

import (
	"context"
	"flag"
	"fmt"
	_ "net/http/pprof"
	"sync"
)

type batchArgs struct {
	batch   int
	batches int
}

func (b *batchArgs) Flags(flags *flag.FlagSet) {
	flags.IntVar(&b.batches, "batches", 1, "number of batches")
	flags.IntVar(&b.batch, "batch", -1, "only do `N`th batch (-1 = all)")
}

func (b *batchArgs) Args(batch int) []string {
	return []string{
		fmt.Sprintf("-batches=%d", b.batches),
		fmt.Sprintf("-batch=%d", batch),
	}
}

// RunBatches calls runFunc once per batch, and returns a slice of
// return values and the first returned error, if any.
func (b *batchArgs) RunBatches(ctx context.Context, runFunc func(context.Context, int) (string, error)) ([]string, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	outputs := make([]string, b.batches)
	var wg WaitGroup
	for batch := 0; batch < b.batches; batch++ {
		if b.batch >= 0 && b.batch != batch {
			continue
		}
		batch := batch
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := runFunc(ctx, batch)
			outputs[batch] = out
			if err != nil {
				wg.Error(err)
				cancel()
			}
		}()
	}
	err := wg.Wait()
	if b.batch >= 0 {
		outputs = outputs[b.batch : b.batch+1]
	}
	return outputs, err
}

func (b *batchArgs) Slice(in []string) []string {
	if b.batches == 0 || b.batch < 0 {
		return in
	}
	batchsize := (len(in) + b.batches - 1) / b.batches
	out := in[batchsize*b.batch:]
	if len(out) > batchsize {
		out = out[:batchsize]
	}
	return out
}

type WaitGroup struct {
	sync.WaitGroup
	err     error
	errOnce sync.Once
}

func (wg *WaitGroup) Error(err error) {
	if err != nil {
		wg.errOnce.Do(func() { wg.err = err })
	}
}

func (wg *WaitGroup) Wait() error {
	wg.WaitGroup.Wait()
	return wg.err
}
