// Copyright (C) The Lightning Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package lightning

import (
	"sync"
	"sync/atomic"
)

type throttle struct {
	Max       int
	wg        sync.WaitGroup
	ch        chan bool
	err       atomic.Value
	setupOnce sync.Once
	errorOnce sync.Once
}

func (t *throttle) Acquire() {
	t.setupOnce.Do(func() {
		if t.Max < 1 {
			panic("throttle.Max < 1")
		}
		t.ch = make(chan bool, t.Max)
	})
	t.wg.Add(1)
	t.ch <- true
}

func (t *throttle) Release() {
	t.wg.Done()
	<-t.ch
}

func (t *throttle) Report(err error) {
	if err != nil {
		t.errorOnce.Do(func() { t.err.Store(err) })
	}
}

func (t *throttle) Err() error {
	err, _ := t.err.Load().(error)
	return err
}

func (t *throttle) Wait() error {
	t.wg.Wait()
	return t.Err()
}

func (t *throttle) Go(f func() error) error {
	t.Acquire()
	if t.Err() != nil {
		t.Release()
		return t.Err()
	}
	go func() {
		t.Report(f())
		t.Release()
	}()
	return nil
}
