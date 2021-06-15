package lightning

import (
	"bufio"
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/klauspost/pgzip"
	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/blake2b"
)

type tileVariantID uint16 // 1-based

type tileLibRef struct {
	Tag     tagID
	Variant tileVariantID
}

type tileSeq map[string][]tileLibRef

func (tseq tileSeq) Variants() ([]tileVariantID, int, int) {
	maxtag := 0
	for _, refs := range tseq {
		for _, ref := range refs {
			if maxtag < int(ref.Tag) {
				maxtag = int(ref.Tag)
			}
		}
	}
	vars := make([]tileVariantID, maxtag+1)
	var kept, dropped int
	for _, refs := range tseq {
		for _, ref := range refs {
			if vars[int(ref.Tag)] != 0 {
				dropped++
			} else {
				kept++
			}
			vars[int(ref.Tag)] = ref.Variant
		}
	}
	return vars, kept, dropped
}

type tileLibrary struct {
	retainNoCalls       bool
	skipOOO             bool
	retainTileSequences bool

	taglib         *tagLibrary
	variant        [][][blake2b.Size256]byte
	refseqs        map[string]map[string][]tileLibRef
	compactGenomes map[string][]tileVariantID
	// count [][]int
	seq      map[[blake2b.Size256]byte][]byte
	variants int64
	// if non-nil, write out any tile variants added while tiling
	encoder *gob.Encoder

	mtx   sync.RWMutex
	vlock []sync.Locker
}

func (tilelib *tileLibrary) loadTagSet(newtagset [][]byte) error {
	// Loading a tagset means either passing it through to the
	// output (if it's the first one we've seen), or just ensuring
	// it doesn't disagree with what we already have.
	if len(newtagset) == 0 {
		return nil
	}
	tilelib.mtx.Lock()
	defer tilelib.mtx.Unlock()
	if tilelib.taglib == nil || tilelib.taglib.Len() == 0 {
		tilelib.taglib = &tagLibrary{}
		err := tilelib.taglib.setTags(newtagset)
		if err != nil {
			return err
		}
		if tilelib.encoder != nil {
			err = tilelib.encoder.Encode(LibraryEntry{
				TagSet: newtagset,
			})
			if err != nil {
				return err
			}
		}
	} else if tilelib.taglib.Len() != len(newtagset) {
		return fmt.Errorf("cannot merge libraries with differing tagsets")
	} else {
		current := tilelib.taglib.Tags()
		for i := range newtagset {
			if !bytes.Equal(newtagset[i], current[i]) {
				return fmt.Errorf("cannot merge libraries with differing tagsets")
			}
		}
	}
	return nil
}

func (tilelib *tileLibrary) loadTileVariants(tvs []TileVariant, variantmap map[tileLibRef]tileVariantID) error {
	for _, tv := range tvs {
		// Assign a new variant ID (unique across all inputs)
		// for each input variant.
		variantmap[tileLibRef{Tag: tv.Tag, Variant: tv.Variant}] = tilelib.getRef(tv.Tag, tv.Sequence).Variant
	}
	return nil
}

func (tilelib *tileLibrary) loadCompactGenomes(cgs []CompactGenome, variantmap map[tileLibRef]tileVariantID, onLoadGenome func(CompactGenome)) error {
	log.Debugf("loadCompactGenomes: %d", len(cgs))
	var wg sync.WaitGroup
	errs := make(chan error, 1)
	for _, cg := range cgs {
		wg.Add(1)
		cg := cg
		go func() {
			defer wg.Done()
			for i, variant := range cg.Variants {
				if len(errs) > 0 {
					return
				}
				if variant == 0 {
					continue
				}
				tag := tagID(i / 2)
				newvariant, ok := variantmap[tileLibRef{Tag: tag, Variant: variant}]
				if !ok {
					err := fmt.Errorf("oops: genome %q has variant %d for tag %d, but that variant was not in its library", cg.Name, variant, tag)
					select {
					case errs <- err:
					default:
					}
					return
				}
				log.Tracef("loadCompactGenomes: cg %s tag %d variant %d => %d", cg.Name, tag, variant, newvariant)
				cg.Variants[i] = newvariant
			}
			if onLoadGenome != nil {
				onLoadGenome(cg)
			}
			if tilelib.encoder != nil {
				err := tilelib.encoder.Encode(LibraryEntry{
					CompactGenomes: []CompactGenome{cg},
				})
				if err != nil {
					select {
					case errs <- err:
					default:
					}
					return
				}
			}
			if tilelib.compactGenomes != nil {
				tilelib.mtx.Lock()
				defer tilelib.mtx.Unlock()
				tilelib.compactGenomes[cg.Name] = cg.Variants
			}
		}()
	}
	wg.Wait()
	go close(errs)
	return <-errs
}

func (tilelib *tileLibrary) loadCompactSequences(cseqs []CompactSequence, variantmap map[tileLibRef]tileVariantID) error {
	log.Debugf("loadCompactSequences: %d", len(cseqs))
	for _, cseq := range cseqs {
		for _, tseq := range cseq.TileSequences {
			for i, libref := range tseq {
				if libref.Variant == 0 {
					// No variant (e.g., import
					// dropped tiles with
					// no-calls) = no translation.
					continue
				}
				v, ok := variantmap[libref]
				if !ok {
					return fmt.Errorf("oops: CompactSequence %q has variant %d for tag %d, but that variant was not in its library", cseq.Name, libref.Variant, libref.Tag)
				}
				tseq[i].Variant = v
			}
		}
		if tilelib.encoder != nil {
			if err := tilelib.encoder.Encode(LibraryEntry{
				CompactSequences: []CompactSequence{cseq},
			}); err != nil {
				return err
			}
		}
	}
	tilelib.mtx.Lock()
	defer tilelib.mtx.Unlock()
	if tilelib.refseqs == nil {
		tilelib.refseqs = map[string]map[string][]tileLibRef{}
	}
	for _, cseq := range cseqs {
		tilelib.refseqs[cseq.Name] = cseq.TileSequences
	}
	return nil
}

func (tilelib *tileLibrary) LoadDir(ctx context.Context, path string, onLoadGenome func(CompactGenome)) error {
	var files []string
	var walk func(string) error
	walk = func(path string) error {
		f, err := open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		fis, err := f.Readdir(-1)
		if err != nil {
			files = append(files, path)
			return nil
		}
		for _, fi := range fis {
			if fi.Name() == "." || fi.Name() == ".." {
				continue
			} else if child := path + "/" + fi.Name(); fi.IsDir() {
				err = walk(child)
				if err != nil {
					return err
				}
			} else if strings.HasSuffix(child, ".gob") || strings.HasSuffix(child, ".gob.gz") {
				files = append(files, child)
			}
		}
		return nil
	}
	log.Infof("LoadDir: walk dir %s", path)
	err := walk(path)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var mtx sync.Mutex
	cgs := []CompactGenome{}
	cseqs := []CompactSequence{}
	variantmap := map[tileLibRef]tileVariantID{}
	errs := make(chan error, len(files))
	log.Infof("LoadDir: read %d files", len(files))
	for _, path := range files {
		path := path
		go func() {
			f, err := open(path)
			if err != nil {
				errs <- err
				return
			}
			defer f.Close()
			errs <- DecodeLibrary(f, strings.HasSuffix(path, ".gz"), func(ent *LibraryEntry) error {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				mtx.Lock()
				defer mtx.Unlock()
				if tilelib.taglib == nil || tilelib.taglib.Len() != len(ent.TagSet) {
					// load first set of tags, or
					// report mismatch if 2 sets
					// have different #tags.
					if err := tilelib.loadTagSet(ent.TagSet); err != nil {
						return err
					}
				}
				if err := tilelib.loadTileVariants(ent.TileVariants, variantmap); err != nil {
					return err
				}
				cgs = append(cgs, ent.CompactGenomes...)
				cseqs = append(cseqs, ent.CompactSequences...)
				return nil
			})
		}()
	}
	for range files {
		err := <-errs
		if err != nil {
			return err
		}
	}
	log.Info("LoadDir: loadCompactGenomes")
	err = tilelib.loadCompactGenomes(cgs, variantmap, onLoadGenome)
	if err != nil {
		return err
	}
	log.Info("LoadDir: loadCompactSequences")
	err = tilelib.loadCompactSequences(cseqs, variantmap)
	if err != nil {
		return err
	}
	log.Info("LoadDir done")
	return nil
}

func (tilelib *tileLibrary) WriteDir(dir string) error {
	nfiles := 128
	files := make([]*os.File, nfiles)
	for i := range files {
		f, err := os.OpenFile(fmt.Sprintf("%s/library.%04d.gob.gz", dir, i), os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			return err
		}
		defer f.Close()
		files[i] = f
	}
	bufws := make([]*bufio.Writer, nfiles)
	for i := range bufws {
		bufws[i] = bufio.NewWriterSize(files[i], 1<<26)
	}
	zws := make([]*pgzip.Writer, nfiles)
	for i := range zws {
		zws[i] = pgzip.NewWriter(bufws[i])
		defer zws[i].Close()
	}
	encoders := make([]*gob.Encoder, nfiles)
	for i := range encoders {
		encoders[i] = gob.NewEncoder(zws[i])
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errs := make(chan error, nfiles)
	for start := range files {
		start := start
		go func() {
			err := encoders[start].Encode(LibraryEntry{TagSet: tilelib.taglib.Tags()})
			if err != nil {
				errs <- err
				return
			}
			if start == 0 {
				// For now, just write all the genomes and refs
				// to the first file
				for name, cg := range tilelib.compactGenomes {
					err := encoders[start].Encode(LibraryEntry{CompactGenomes: []CompactGenome{{
						Name:     name,
						Variants: cg,
					}}})
					if err != nil {
						errs <- err
						return
					}
				}
				for name, tseqs := range tilelib.refseqs {
					err := encoders[start].Encode(LibraryEntry{CompactSequences: []CompactSequence{{
						Name:          name,
						TileSequences: tseqs,
					}}})
					if err != nil {
						errs <- err
						return
					}
				}
			}
			tvs := []TileVariant{}
			for tag := start; tag < len(tilelib.variant) && ctx.Err() == nil; tag += nfiles {
				tvs = tvs[:0]
				for idx, hash := range tilelib.variant[tag] {
					tvs = append(tvs, TileVariant{
						Tag:      tagID(tag),
						Variant:  tileVariantID(idx + 1),
						Blake2b:  hash,
						Sequence: tilelib.seq[hash],
					})
				}
				err := encoders[start].Encode(LibraryEntry{TileVariants: tvs})
				if err != nil {
					errs <- err
					return
				}
			}
			errs <- nil
		}()
	}
	for range files {
		err := <-errs
		if err != nil {
			return err
		}
	}
	for i := range zws {
		err := zws[i].Close()
		if err != nil {
			return err
		}
		err = bufws[i].Flush()
		if err != nil {
			return err
		}
		err = files[i].Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// Load library data from rdr. Tile variants might be renumbered in
// the process; in that case, genomes variants will be renumbered to
// match.
//
// If onLoadGenome is non-nil, call it on each CompactGenome entry.
func (tilelib *tileLibrary) LoadGob(ctx context.Context, rdr io.Reader, gz bool, onLoadGenome func(CompactGenome)) error {
	cgs := []CompactGenome{}
	cseqs := []CompactSequence{}
	variantmap := map[tileLibRef]tileVariantID{}
	err := DecodeLibrary(rdr, gz, func(ent *LibraryEntry) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := tilelib.loadTagSet(ent.TagSet); err != nil {
			return err
		}
		if err := tilelib.loadTileVariants(ent.TileVariants, variantmap); err != nil {
			return err
		}
		cgs = append(cgs, ent.CompactGenomes...)
		cseqs = append(cseqs, ent.CompactSequences...)
		return nil
	})
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	err = tilelib.loadCompactGenomes(cgs, variantmap, onLoadGenome)
	if err != nil {
		return err
	}
	err = tilelib.loadCompactSequences(cseqs, variantmap)
	if err != nil {
		return err
	}
	return nil
}

func (tilelib *tileLibrary) dump(out io.Writer) {
	printTV := func(tag int, variant tileVariantID) {
		if variant < 1 {
			fmt.Fprintf(out, " -")
		} else if tag >= len(tilelib.variant) {
			fmt.Fprintf(out, " (!tag=%d)", tag)
		} else if int(variant) > len(tilelib.variant[tag]) {
			fmt.Fprintf(out, " (tag=%d,!variant=%d)", tag, variant)
		} else {
			fmt.Fprintf(out, " %x", tilelib.variant[tag][variant-1][:8])
		}
	}
	for refname, refseqs := range tilelib.refseqs {
		for seqname, seq := range refseqs {
			fmt.Fprintf(out, "ref %s %s", refname, seqname)
			for _, libref := range seq {
				printTV(int(libref.Tag), libref.Variant)
			}
			fmt.Fprintf(out, "\n")
		}
	}
	for name, cg := range tilelib.compactGenomes {
		fmt.Fprintf(out, "cg %s", name)
		for tag, variant := range cg {
			printTV(tag/2, variant)
		}
		fmt.Fprintf(out, "\n")
	}
}

type importStats struct {
	InputFile              string
	InputLabel             string
	InputLength            int
	InputCoverage          int
	PathLength             int
	DroppedOutOfOrderTiles int
}

func (tilelib *tileLibrary) TileFasta(filelabel string, rdr io.Reader, matchChromosome *regexp.Regexp) (tileSeq, []importStats, error) {
	ret := tileSeq{}
	type jobT struct {
		label string
		fasta []byte
	}
	todo := make(chan jobT, 1)
	scanner := bufio.NewScanner(rdr)
	go func() {
		defer close(todo)
		var fasta []byte
		var seqlabel string
		for scanner.Scan() {
			buf := scanner.Bytes()
			if len(buf) > 0 && buf[0] == '>' {
				todo <- jobT{seqlabel, append([]byte(nil), fasta...)}
				seqlabel, fasta = strings.SplitN(string(buf[1:]), " ", 2)[0], fasta[:0]
				log.Debugf("%s %s reading fasta", filelabel, seqlabel)
			} else {
				fasta = append(fasta, bytes.ToLower(buf)...)
			}
		}
		todo <- jobT{seqlabel, fasta}
	}()
	type foundtag struct {
		pos   int
		tagid tagID
	}
	found := make([]foundtag, 2000000)
	path := make([]tileLibRef, 2000000)
	totalFoundTags := 0
	totalPathLen := 0
	skippedSequences := 0
	taglen := tilelib.taglib.TagLen()
	var stats []importStats
	for job := range todo {
		if len(job.fasta) == 0 {
			continue
		} else if !matchChromosome.MatchString(job.label) {
			skippedSequences++
			continue
		}
		log.Debugf("%s %s tiling", filelabel, job.label)

		found = found[:0]
		tilelib.taglib.FindAll(job.fasta, func(tagid tagID, pos, taglen int) {
			found = append(found, foundtag{pos: pos, tagid: tagid})
		})
		totalFoundTags += len(found)
		if len(found) == 0 {
			log.Warnf("%s %s no tags found", filelabel, job.label)
		}

		skipped := 0
		if tilelib.skipOOO {
			log.Infof("%s %s keeping longest increasing subsequence", filelabel, job.label)
			keep := longestIncreasingSubsequence(len(found), func(i int) int { return int(found[i].tagid) })
			for i, x := range keep {
				found[i] = found[x]
			}
			skipped = len(found) - len(keep)
			found = found[:len(keep)]
		}

		log.Infof("%s %s getting %d librefs", filelabel, job.label, len(found))
		throttle := &throttle{Max: runtime.NumCPU()}
		path = path[:len(found)]
		var lowquality int64
		for i, f := range found {
			i, f := i, f
			throttle.Acquire()
			go func() {
				defer throttle.Release()
				var startpos, endpos int
				if i == 0 {
					startpos = 0
				} else {
					startpos = f.pos
				}
				if i == len(found)-1 {
					endpos = len(job.fasta)
				} else {
					endpos = found[i+1].pos + taglen
				}
				path[i] = tilelib.getRef(f.tagid, job.fasta[startpos:endpos])
				if countBases(job.fasta[startpos:endpos]) != endpos-startpos {
					atomic.AddInt64(&lowquality, 1)
				}
			}()
		}
		throttle.Wait()

		log.Infof("%s %s copying path", filelabel, job.label)

		pathcopy := make([]tileLibRef, len(path))
		copy(pathcopy, path)
		ret[job.label] = pathcopy

		basesIn := countBases(job.fasta)
		log.Infof("%s %s fasta in %d coverage in %d path len %d low-quality %d skipped-out-of-order %d", filelabel, job.label, len(job.fasta), basesIn, len(path), lowquality, skipped)
		stats = append(stats, importStats{
			InputFile:              filelabel,
			InputLabel:             job.label,
			InputLength:            len(job.fasta),
			InputCoverage:          basesIn,
			PathLength:             len(path),
			DroppedOutOfOrderTiles: skipped,
		})

		totalPathLen += len(path)
	}
	log.Printf("%s tiled with total path len %d in %d sequences (skipped %d sequences that did not match chromosome regexp, skipped %d out-of-order tags)", filelabel, totalPathLen, len(ret), skippedSequences, totalFoundTags-totalPathLen)
	return ret, stats, scanner.Err()
}

func (tilelib *tileLibrary) Len() int64 {
	return atomic.LoadInt64(&tilelib.variants)
}

// Return a tileLibRef for a tile with the given tag and sequence,
// adding the sequence to the library if needed.
func (tilelib *tileLibrary) getRef(tag tagID, seq []byte) tileLibRef {
	dropSeq := false
	if !tilelib.retainNoCalls {
		for _, b := range seq {
			if b != 'a' && b != 'c' && b != 'g' && b != 't' {
				dropSeq = true
				break
			}
		}
	}
	seqhash := blake2b.Sum256(seq)
	var vlock sync.Locker

	tilelib.mtx.RLock()
	if len(tilelib.vlock) > int(tag) {
		vlock = tilelib.vlock[tag]
	}
	tilelib.mtx.RUnlock()

	if vlock != nil {
		vlock.Lock()
		for i, varhash := range tilelib.variant[tag] {
			if varhash == seqhash {
				vlock.Unlock()
				return tileLibRef{Tag: tag, Variant: tileVariantID(i + 1)}
			}
		}
		vlock.Unlock()
	} else {
		tilelib.mtx.Lock()
		if tilelib.variant == nil && tilelib.taglib != nil {
			tilelib.variant = make([][][blake2b.Size256]byte, tilelib.taglib.Len())
			tilelib.vlock = make([]sync.Locker, tilelib.taglib.Len())
			for i := range tilelib.vlock {
				tilelib.vlock[i] = new(sync.Mutex)
			}
		}
		if int(tag) >= len(tilelib.variant) {
			oldlen := len(tilelib.vlock)
			for i := 0; i < oldlen; i++ {
				tilelib.vlock[i].Lock()
			}
			// If we haven't seen the tag library yet (as
			// in a merge), tilelib.taglib.Len() is
			// zero. We can still behave correctly, we
			// just need to expand the tilelib.variant and
			// tilelib.vlock slices as needed.
			if int(tag) >= cap(tilelib.variant) {
				// Allocate 2x capacity.
				newslice := make([][][blake2b.Size256]byte, int(tag)+1, (int(tag)+1)*2)
				copy(newslice, tilelib.variant)
				tilelib.variant = newslice[:int(tag)+1]
				newvlock := make([]sync.Locker, int(tag)+1, (int(tag)+1)*2)
				copy(newvlock, tilelib.vlock)
				tilelib.vlock = newvlock[:int(tag)+1]
			} else {
				// Use previously allocated capacity,
				// avoiding copy.
				tilelib.variant = tilelib.variant[:int(tag)+1]
				tilelib.vlock = tilelib.vlock[:int(tag)+1]
			}
			for i := oldlen; i < len(tilelib.vlock); i++ {
				tilelib.vlock[i] = new(sync.Mutex)
			}
			for i := 0; i < oldlen; i++ {
				tilelib.vlock[i].Unlock()
			}
		}
		vlock = tilelib.vlock[tag]
		tilelib.mtx.Unlock()
	}

	vlock.Lock()
	for i, varhash := range tilelib.variant[tag] {
		if varhash == seqhash {
			vlock.Unlock()
			return tileLibRef{Tag: tag, Variant: tileVariantID(i + 1)}
		}
	}
	atomic.AddInt64(&tilelib.variants, 1)
	tilelib.variant[tag] = append(tilelib.variant[tag], seqhash)
	variant := tileVariantID(len(tilelib.variant[tag]))
	vlock.Unlock()

	if tilelib.retainTileSequences && !dropSeq {
		tilelib.mtx.Lock()
		if tilelib.seq == nil {
			tilelib.seq = map[[blake2b.Size256]byte][]byte{}
		}
		tilelib.seq[seqhash] = append([]byte(nil), seq...)
		tilelib.mtx.Unlock()
	}

	if tilelib.encoder != nil {
		saveSeq := seq
		if dropSeq {
			// Save the hash, but not the sequence
			saveSeq = nil
		}
		tilelib.encoder.Encode(LibraryEntry{
			TileVariants: []TileVariant{{
				Tag:      tag,
				Variant:  variant,
				Blake2b:  seqhash,
				Sequence: saveSeq,
			}},
		})
	}
	return tileLibRef{Tag: tag, Variant: variant}
}

func (tilelib *tileLibrary) TileVariantSequence(libref tileLibRef) []byte {
	if libref.Variant == 0 || len(tilelib.variant) <= int(libref.Tag) || len(tilelib.variant[libref.Tag]) < int(libref.Variant) {
		return nil
	}
	return tilelib.seq[tilelib.variant[libref.Tag][libref.Variant-1]]
}

// Tidy deletes unreferenced tile variants and renumbers variants so
// more common variants have smaller IDs.
func (tilelib *tileLibrary) Tidy() {
	log.Print("Tidy: compute inref")
	inref := map[tileLibRef]bool{}
	for _, refseq := range tilelib.refseqs {
		for _, librefs := range refseq {
			for _, libref := range librefs {
				inref[libref] = true
			}
		}
	}
	log.Print("Tidy: compute remap")
	remap := make([][]tileVariantID, len(tilelib.variant))
	throttle := throttle{Max: runtime.NumCPU() + 1}
	for tag, oldvariants := range tilelib.variant {
		tag, oldvariants := tagID(tag), oldvariants
		if tag%1000000 == 0 {
			log.Printf("Tidy: tag %d", tag)
		}
		throttle.Acquire()
		go func() {
			defer throttle.Release()
			uses := make([]int, len(oldvariants))
			for _, cg := range tilelib.compactGenomes {
				for phase := 0; phase < 2; phase++ {
					cgi := int(tag)*2 + phase
					if cgi < len(cg) && cg[cgi] > 0 {
						uses[cg[cgi]-1]++
					}
				}
			}

			// Compute desired order of variants:
			// neworder[x] == index in oldvariants that
			// should move to position x.
			neworder := make([]int, len(oldvariants))
			for i := range neworder {
				neworder[i] = i
			}
			sort.Slice(neworder, func(i, j int) bool {
				if cmp := uses[neworder[i]] - uses[neworder[j]]; cmp != 0 {
					return cmp > 0
				} else {
					return bytes.Compare(oldvariants[neworder[i]][:], oldvariants[neworder[j]][:]) < 0
				}
			})

			// Replace tilelib.variant[tag] with a new
			// re-ordered slice of hashes, and make a
			// mapping from old to new variant IDs.
			remaptag := make([]tileVariantID, len(oldvariants)+1)
			newvariants := make([][blake2b.Size256]byte, 0, len(neworder))
			for _, oldi := range neworder {
				if uses[oldi] > 0 || inref[tileLibRef{Tag: tag, Variant: tileVariantID(oldi + 1)}] {
					newvariants = append(newvariants, oldvariants[oldi])
					remaptag[oldi+1] = tileVariantID(len(newvariants))
				}
			}
			tilelib.variant[tag] = newvariants
			remap[tag] = remaptag
		}()
	}
	throttle.Wait()

	// Apply remap to genomes and reference sequences, so they
	// refer to the same tile variants using the changed IDs.
	log.Print("Tidy: apply remap")
	var wg sync.WaitGroup
	for _, cg := range tilelib.compactGenomes {
		cg := cg
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx, variant := range cg {
				cg[idx] = remap[tagID(idx/2)][variant]
			}
		}()
	}
	for _, refcs := range tilelib.refseqs {
		for _, refseq := range refcs {
			refseq := refseq
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i, tv := range refseq {
					refseq[i].Variant = remap[tv.Tag][tv.Variant]
				}
			}()
		}
	}
	wg.Wait()
	log.Print("Tidy: done")
}

func countBases(seq []byte) int {
	n := 0
	for _, c := range seq {
		if isbase[c] {
			n++
		}
	}
	return n
}
