package lightning

import (
	"math/rand"
	"testing"

	"gopkg.in/check.v1"
)

type maskSuite struct{}

var _ = check.Suite(&maskSuite{})

func (s *maskSuite) TestMask(c *check.C) {
	m := mask{}
	for i := 0; i < 1000000; i++ {
		start := rand.Int() % 100000
		end := rand.Int()%100000 + start
		if start <= 9000 && end >= 8000 ||
			start <= 8 && end >= 4 ||
			start <= 1 {
			continue
		}
		m.Add("chr1", start, end)
	}
	m.Add("chr1", 1200, 3400)
	m.Add("chr1", 5600, 7800)
	m.Add("chr1", 5300, 7900)
	m.Add("chr1", 9900, 9900)
	m.Add("chr1", 1, 1)
	m.Add("chr1", 0, 0)
	m.Add("chr1", 2, 2)
	m.Add("chr1", 9, 9)
	m.Freeze()
	c.Check(m.Check("chr1", 1, 1), check.Equals, true)
	c.Check(m.Check("chr1", 4, 8), check.Equals, false)
	c.Check(m.Check("chr1", 7800, 8000), check.Equals, true)
	c.Check(m.Check("chr1", 8000, 9000), check.Equals, false)
	c.Check(m.Check("chr1999", 1, 1), check.Equals, false)
}

func BenchmarkMask1000(b *testing.B) {
	benchmarkMask(b, 1000)
}

func BenchmarkMask10000(b *testing.B) {
	benchmarkMask(b, 10000)
}

func BenchmarkMask100000(b *testing.B) {
	benchmarkMask(b, 100000)
}

func BenchmarkMask1000000(b *testing.B) {
	benchmarkMask(b, 1000000)
}

func benchmarkMask(b *testing.B, size int) {
	m := mask{}
	for i := 0; i < size; i++ {
		start := rand.Int() % 10000000
		end := rand.Int()%300 + start
		m.Add("chrB", start, end)
	}
	m.Freeze()
	for n := 0; n < b.N; n++ {
		start := rand.Int() % 10000000
		end := rand.Int()%300 + start
		m.Check("chrB", start, end)
	}
}
