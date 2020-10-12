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
	var ret []CompactGenome
	err := DecodeLibrary(rdr, func(ent *LibraryEntry) error {
		ret = append(ret, ent.CompactGenomes...)
		return nil
	})
	return ret, err
}

func DecodeLibrary(rdr io.Reader, cb func(*LibraryEntry) error) error {
	dec := gob.NewDecoder(bufio.NewReaderSize(rdr, 1<<26))
	var err error
	for err == nil {
		var ent LibraryEntry
		err = dec.Decode(&ent)
		if err == nil {
			err = cb(&ent)
		}
	}
	if err == io.EOF {
		return nil
	} else {
		return err
	}
}
