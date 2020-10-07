package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
)

const tagmapKeySize = 32

type tagmapKey uint64

type tagID int32

type tagInfo struct {
	id     tagID // 0-based position in input tagset
	tagseq []byte
}

type tagLibrary struct {
	tagmap  map[tagmapKey]tagInfo
	keylen  int
	keymask tagmapKey
}

func (taglib *tagLibrary) Load(rdr io.Reader) error {
	var seqs [][]byte
	scanner := bufio.NewScanner(rdr)
	for scanner.Scan() {
		data := scanner.Bytes()
		if len(data) > 0 && data[0] == '>' {
		} else {
			seqs = append(seqs, append([]byte(nil), data...))
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return taglib.setTags(seqs)
}

func (taglib *tagLibrary) FindAll(buf []byte, fn func(id tagID, pos, taglen int)) {
	var key tagmapKey
	valid := 0 // if valid < taglib.keylen, key has "no data" zeroes that are otherwise indistinguishable from "A"
	for i, base := range buf {
		if !isbase[int(base)] {
			valid = 0
			continue
		}
		key = ((key << 2) | twobit[int(base)]) & taglib.keymask
		valid++

		if valid < taglib.keylen {
			continue
		} else if taginfo, ok := taglib.tagmap[key]; !ok {
			continue
		} else if tagstart := i - taglib.keylen + 1; len(taginfo.tagseq) > taglib.keylen && (len(buf) < i+len(taginfo.tagseq) || !bytes.Equal(taginfo.tagseq, buf[i:i+len(taginfo.tagseq)])) {
			// key portion matches, but not the entire tag
			continue
		} else {
			fn(taginfo.id, tagstart, len(taginfo.tagseq))
			valid = 0 // don't try to match overlapping tags
		}
	}
}

func (taglib *tagLibrary) Len() int {
	return len(taglib.tagmap)
}

var (
	twobit = func() []tagmapKey {
		r := make([]tagmapKey, 256)
		r[int('a')] = 0
		r[int('A')] = 0
		r[int('c')] = 1
		r[int('C')] = 1
		r[int('g')] = 2
		r[int('G')] = 2
		r[int('t')] = 3
		r[int('T')] = 3
		return r
	}()
	isbase = func() []bool {
		r := make([]bool, 256)
		r[int('a')] = true
		r[int('A')] = true
		r[int('c')] = true
		r[int('C')] = true
		r[int('g')] = true
		r[int('G')] = true
		r[int('t')] = true
		r[int('T')] = true
		return r
	}()
)

func (taglib *tagLibrary) setTags(tags [][]byte) error {
	taglib.keylen = tagmapKeySize
	for _, t := range tags {
		if l := len(t); taglib.keylen > l {
			taglib.keylen = l
		}
	}
	taglib.keymask = tagmapKey((1 << (taglib.keylen * 2)) - 1)
	taglib.tagmap = map[tagmapKey]tagInfo{}
	for i, tag := range tags {
		var key tagmapKey
		for _, b := range tag[:taglib.keylen] {
			key = (key << 2) | twobit[int(b)]
		}
		if _, ok := taglib.tagmap[key]; ok {
			return fmt.Errorf("first %d bytes of tag %d (%x) are not unique", taglib.keylen, i, key)
		}
		taglib.tagmap[key] = tagInfo{tagID(i), tag}
	}
	return nil
}

func (taglib *tagLibrary) Tags() [][]byte {
	out := make([][]byte, len(taglib.tagmap))
	untwobit := []byte{'a', 'c', 'g', 't'}
	for key, info := range taglib.tagmap {
		seq := make([]byte, taglib.keylen)
		for i := len(seq) - 1; i >= 0; i-- {
			seq[i] = untwobit[int(key)&3]
			key = key >> 2
		}
		out[int(info.id)] = seq
	}
	return out
}
