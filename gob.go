package main

import (
	"bufio"
	"encoding/gob"
	"io"
	_ "net/http/pprof"

	"golang.org/x/crypto/blake2b"
)

type CompactGenome struct {
	Name     string
	Variants []tileVariantID
}

type TileVariant struct {
	Tag      tagID
	Variant  tileVariantID
	Blake2b  [blake2b.Size256]byte
	Sequence []byte
}

type LibraryEntry struct {
	TagSet         [][]byte
	CompactGenomes []CompactGenome
	TileVariants   []TileVariant
}

func ReadCompactGenomes(rdr io.Reader) ([]CompactGenome, error) {
	dec := gob.NewDecoder(bufio.NewReaderSize(rdr, 1<<26))
	var ret []CompactGenome
	for {
		var ent LibraryEntry
		err := dec.Decode(&ent)
		if err == io.EOF {
			return ret, nil
		} else if err != nil {
			return nil, err
		}
		ret = append(ret, ent.CompactGenomes...)
	}
}
