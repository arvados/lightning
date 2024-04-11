// Copyright (C) The Lightning Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package lightning

import (
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	log "github.com/sirupsen/logrus"
)

func writeProfilesPeriodically(outdir string) {
	for range time.NewTicker(time.Minute).C {
		writeMemProfile(outdir)
		writeCPUProfile(outdir)
	}
}

func writeCPUProfile(outdir string) {
	f, err := os.OpenFile(outdir+"/cpu.prof~", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Print(err)
		return
	}
	defer f.Close()
	runtime.GC()
	if err := pprof.StartCPUProfile(f); err != nil {
		log.Print(err)
		return
	}
	time.Sleep(time.Second)
	pprof.StopCPUProfile()
	err = f.Close()
	if err != nil {
		log.Print(err)
		return
	}
	err = os.Rename(outdir+"/cpu.prof~", outdir+"/cpu.prof")
	if err != nil {
		log.Print(err)
	}
	return
}

func writeMemProfile(outdir string) {
	f, err := os.OpenFile(outdir+"/mem.prof~", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Print(err)
		return
	}
	defer f.Close()
	runtime.GC()
	if err := pprof.WriteHeapProfile(f); err != nil {
		log.Print(err)
		return
	}
	err = f.Close()
	if err != nil {
		log.Print(err)
		return
	}
	err = os.Rename(outdir+"/mem.prof~", outdir+"/mem.prof")
	if err != nil {
		log.Print(err)
	}
	return
}
