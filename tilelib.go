package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"io"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/blake2b"
)

type tileVariantID uint16 // 1-based

type tileLibRef struct {
	tag     tagID
	variant tileVariantID
}

type tileSeq map[string][]tileLibRef

func (tseq tileSeq) Variants() ([]tileVariantID, int, int) {
	maxtag := 0
	for _, refs := range tseq {
		for _, ref := range refs {
			if maxtag < int(ref.tag) {
				maxtag = int(ref.tag)
			}
		}
	}
	vars := make([]tileVariantID, maxtag+1)
	var kept, dropped int
	for _, refs := range tseq {
		for _, ref := range refs {
			if vars[int(ref.tag)] != 0 {
				dropped++
			} else {
				kept++
			}
			vars[int(ref.tag)] = ref.variant
		}
	}
	return vars, kept, dropped
}

type tileLibrary struct {
	includeNoCalls bool
	skipOOO        bool
	taglib         *tagLibrary
	variant        [][][blake2b.Size256]byte
	// count [][]int
	// seq map[[blake2b.Size]byte][]byte
	variants int
	// if non-nil, write out any tile variants added while tiling
	encoder *gob.Encoder

	mtx sync.Mutex
}

func (tilelib *tileLibrary) TileFasta(filelabel string, rdr io.Reader) (tileSeq, error) {
	ret := tileSeq{}
	type jobT struct {
		label string
		fasta []byte
	}
	todo := make(chan jobT)
	scanner := bufio.NewScanner(rdr)
	go func() {
		defer close(todo)
		var fasta []byte
		var seqlabel string
		for scanner.Scan() {
			buf := scanner.Bytes()
			if len(buf) > 0 && buf[0] == '>' {
				todo <- jobT{seqlabel, fasta}
				seqlabel, fasta = string(buf[1:]), nil
				log.Debugf("%s %s reading fasta", filelabel, seqlabel)
			} else {
				fasta = append(fasta, bytes.ToLower(buf)...)
			}
		}
		todo <- jobT{seqlabel, fasta}
	}()
	type foundtag struct {
		pos    int
		tagid  tagID
		taglen int
	}
	found := make([]foundtag, 2000000)
	path := make([]tileLibRef, 2000000)
	totalFoundTags := 0
	totalPathLen := 0
	skippedSequences := 0
	for job := range todo {
		if len(job.fasta) == 0 {
			continue
		} else if strings.Contains(job.label, "_") {
			skippedSequences++
			continue
		}
		log.Debugf("%s %s tiling", filelabel, job.label)

		found = found[:0]
		tilelib.taglib.FindAll(job.fasta, func(tagid tagID, pos, taglen int) {
			found = append(found, foundtag{pos: pos, tagid: tagid, taglen: taglen})
		})
		totalFoundTags += len(found)

		skipped := 0
		path = path[:0]
		last := foundtag{tagid: -1}
		if tilelib.skipOOO {
			keep := longestIncreasingSubsequence(len(found), func(i int) int { return int(found[i].tagid) })
			for i, x := range keep {
				found[i] = found[x]
			}
			skipped = len(found) - len(keep)
			found = found[:len(keep)]
		}
		for i, f := range found {
			log.Tracef("%s %s found[%d] == %#v", filelabel, job.label, i, f)
			if last.taglen > 0 {
				path = append(path, tilelib.getRef(last.tagid, job.fasta[last.pos:f.pos+f.taglen]))
			}
			last = f
		}
		if last.taglen > 0 {
			path = append(path, tilelib.getRef(last.tagid, job.fasta[last.pos:]))
		}

		pathcopy := make([]tileLibRef, len(path))
		copy(pathcopy, path)
		ret[job.label] = pathcopy
		log.Debugf("%s %s tiled with path len %d, skipped %d", filelabel, job.label, len(path), skipped)
		totalPathLen += len(path)
	}
	log.Printf("%s tiled with total path len %d in %d sequences (skipped %d sequences with '_' in name, skipped %d out-of-order tags)", filelabel, totalPathLen, len(ret), skippedSequences, totalFoundTags-totalPathLen)
	return ret, scanner.Err()
}

func (tilelib *tileLibrary) Len() int {
	tilelib.mtx.Lock()
	defer tilelib.mtx.Unlock()
	return tilelib.variants
}

// Return a tileLibRef for a tile with the given tag and sequence,
// adding the sequence to the library if needed.
func (tilelib *tileLibrary) getRef(tag tagID, seq []byte) tileLibRef {
	if !tilelib.includeNoCalls {
		for _, b := range seq {
			if b != 'a' && b != 'c' && b != 'g' && b != 't' {
				// return "tile not found" if seq has any
				// no-calls
				return tileLibRef{tag: tag}
			}
		}
	}
	tilelib.mtx.Lock()
	// if tilelib.seq == nil {
	// 	tilelib.seq = map[[blake2b.Size]byte][]byte{}
	// }
	if tilelib.variant == nil {
		tilelib.variant = make([][][blake2b.Size256]byte, tilelib.taglib.Len())
	}
	seqhash := blake2b.Sum256(seq)
	for i, varhash := range tilelib.variant[tag] {
		if varhash == seqhash {
			tilelib.mtx.Unlock()
			return tileLibRef{tag: tag, variant: tileVariantID(i + 1)}
		}
	}
	tilelib.variants++
	tilelib.variant[tag] = append(tilelib.variant[tag], seqhash)
	// tilelib.seq[seqhash] = append([]byte(nil), seq...)
	ret := tileLibRef{tag: tag, variant: tileVariantID(len(tilelib.variant[tag]))}
	tilelib.mtx.Unlock()

	if tilelib.encoder != nil {
		tilelib.encoder.Encode(LibraryEntry{
			TileVariants: []TileVariant{{
				Tag:      tag,
				Blake2b:  seqhash,
				Sequence: seq,
			}},
		})
	}
	return ret
}
