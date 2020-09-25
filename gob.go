package main

import (
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
	Blake2b  [blake2b.Size256]byte
	Sequence []byte
}

type LibraryEntry struct {
	TagSet         [][]byte
	CompactGenomes []CompactGenome
	TileVariants   []TileVariant
}

func ReadCompactGenomes(rdr io.Reader) ([]CompactGenome, error) {
	dec := gob.NewDecoder(rdr)
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
