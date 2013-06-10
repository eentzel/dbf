package dbf

// reference implementation:
//     http://dbf.berlios.de/

// test data: http://abs.gov.au/AUSSTATS/abs@.nsf/DetailsPage/2923.0.30.0012006?OpenDocument

// a dbf.Reader should have some metadata, and a Read() method that returns
// table rows, one at a time

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
)

type Reader struct {
	r io.ReadSeeker // is this the type I want?
	// fields  []string      // probably need to keep name, type, and length around for each field
	year, month, day int
	Length           int // number of records
	fields           []Field
}

type header struct {
	// documented at: http://www.dbase.com/knowledgebase/int/db7_file_fmt.htm
	Version    byte
	Year       uint8 // stored as offset from (decimal) 1900
	Month, Day uint8
	Nrec       uint32
	Headerlen  uint16 // in bytes
	Recordlen  uint16 // length of each record, in bytes
}

func NewReader(r io.ReadSeeker) (*Reader, error) {
	var h header
	err := binary.Read(r, binary.LittleEndian, &h)
	if err != nil || h.Version != 0x03 {
		fmt.Printf("unexepected file version: %d\n", h.Version)
		return nil, err
	}

	fmt.Printf("Header len: %d\nRecord len: %d\n", h.Headerlen, h.Recordlen)

	var fields []Field
	_, err = r.Seek(0x20, 0)
	// fmt.Printf("New offset: %d, error: %v\n", newOff, err)

	var offset uint16
	for offset = 0x20; offset < h.Headerlen-1; offset += 32 {
		f := Field{}
		binary.Read(r, binary.LittleEndian, &f)
		fmt.Printf("new field: %v\n", f)
		f.validate()
		fields = append(fields, f)
	}

	// fmt.Printf("header: %v\n", h)

	br := bufio.NewReader(r)
	if eoh, err := br.ReadByte(); err != nil {
		panic(err)
	} else if eoh != 0x0D {
		log.Fatalf("Header was supposed to be %d bytes long, but found byte %#x at that offset instead of expected byte 0x0D\n", h.Headerlen, eoh)
	}

	return &Reader{r, 1900 + int(h.Year),
		int(h.Month), int(h.Day), int(h.Nrec), fields}, nil
}

func (r *Reader) ModDate() (int, int, int) {
	return r.year, r.month, r.day
}

func (r *Reader) FieldNames() (names []string) {
	for _, f := range r.fields {
		names = append(names, string(f.Name[0:11]))
	}
	return
}

func (f *Field) validate() bool {
	return true
}

type Field struct {
	Name          [11]byte // 0x0 terminated
	Type          byte
	Offset        uint32
	Len           uint8
	DecimalPlaces uint8 // ?
	// Flags         uint8
	// AutoIncrNext  uint32
	// AutoIncrStep  uint8
	Padding [14]byte
}

// http://play.golang.org/p/-CUbdWc6zz
type Record map[string]interface{}

func (r *Reader) Read(i int) (rec Record, err error) {
	// r.r.Seek(dataStart + r.Recordlen*i)
	return
}

func main() {
	var filename = os.Args[1]
	var infile, _ = os.Open(filename)
	r, err := NewReader(infile)
	fmt.Printf("reader:%v\nerr:%v\n", r, err)
	// if err != nil {
	// 	log.Fatalf("Unable to open %s: %s", filename, err)
	// }
	// rec, err := r.Read(1)
}
