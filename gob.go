package lightning

import (
	"bufio"
	"encoding/gob"
	"io"
	"io/ioutil"
	_ "net/http/pprof"

	"github.com/klauspost/pgzip"
	"golang.org/x/crypto/blake2b"
)

type CompactGenome struct {
	Name     string
	Variants []tileVariantID
}

type CompactSequence struct {
	Name          string
	TileSequences map[string][]tileLibRef
}

type TileVariant struct {
	Tag      tagID
	Variant  tileVariantID
	Blake2b  [blake2b.Size256]byte
	Sequence []byte
}

type LibraryEntry struct {
	TagSet           [][]byte
	CompactGenomes   []CompactGenome
	CompactSequences []CompactSequence
	TileVariants     []TileVariant
}

func ReadCompactGenomes(rdr io.Reader, gz bool) ([]CompactGenome, error) {
	var ret []CompactGenome
	err := DecodeLibrary(rdr, gz, func(ent *LibraryEntry) error {
		ret = append(ret, ent.CompactGenomes...)
		return nil
	})
	return ret, err
}

func DecodeLibrary(rdr io.Reader, gz bool, cb func(*LibraryEntry) error) error {
	zrdr := ioutil.NopCloser(rdr)
	var err error
	if gz {
		zrdr, err = pgzip.NewReader(bufio.NewReaderSize(rdr, 1<<20))
		if err != nil {
			return err
		}
	}
	dec := gob.NewDecoder(zrdr)
	for err == nil {
		var ent LibraryEntry
		err = dec.Decode(&ent)
		if err == nil {
			err = cb(&ent)
		}
	}
	if err != io.EOF {
		return err
	}
	return zrdr.Close()
}
