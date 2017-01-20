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
	"strconv"
	"strings"
	"sync"
)

type Reader struct {
	r                io.ReadSeeker
	year, month, day int
	Length           int // number of records
	fields           []Field
	headerlen        uint16 // in bytes
	recordlen        uint16 // length of each record, in bytes
	sync.Mutex
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
	if _, err := r.Seek(0, 0); err != nil {
		return nil, err
	}
	err := binary.Read(r, binary.LittleEndian, &h)
	if err != nil {
		return nil, err
	} else if h.Version != 0x03 {
		return nil, fmt.Errorf("unexepected file version: %d\n", h.Version)
	}

	var fields []Field
	if _, err := r.Seek(0x20, 0); err != nil {
		return nil, err
	}
	var offset uint16
	for offset = 0x20; offset < h.Headerlen-1; offset += 32 {
		f := Field{}
		binary.Read(r, binary.LittleEndian, &f)
		if err = f.validate(); err != nil {
			return nil, err
		}
		fields = append(fields, f)
	}

	br := bufio.NewReader(r)
	if eoh, err := br.ReadByte(); err != nil {
		return nil, err
	} else if eoh != 0x0D {
		return nil, fmt.Errorf("Header was supposed to be %d bytes long, but found byte %#x at that offset instead of expected byte 0x0D\n", h.Headerlen, eoh)
	}

	return &Reader{r, 1900 + int(h.Year),
		int(h.Month), int(h.Day), int(h.Nrec), fields,
		h.Headerlen, h.Recordlen, *new(sync.Mutex)}, nil
}

func (r *Reader) ModDate() (int, int, int) {
	return r.year, r.month, r.day
}

//FieldName - returns field name, read up to the first "\x00" rune, to accomodate some malformed dbf
func (r *Reader) FieldName(i int) (name string) {
	for _, val := range string(r.fields[i].Name[:]) {
	if val == 0 {
		return
	}
	name = name + string(val)
	}
	return
}

//FieldNames - return the full list of Field Names
func (r *Reader) FieldNames() (names []string) {
	for i := range r.fields {
		names = append(names, r.FieldName(i))
	}
	return
}

func (f *Field) validate() error {
	switch f.Type {
	case 'C', 'N', 'F':
		return nil
	}
	return fmt.Errorf("Sorry, dbf library doesn't recognize field type '%c'", f.Type)
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
	_ [14]byte
}

// http://play.golang.org/p/-CUbdWc6zz
type Record map[string]interface{}

//Read - read record i
func (r *Reader) Read(i int) (rec Record, err error) {
	r.Lock()
	defer r.Unlock()

	offset := int64(r.headerlen) + int64(r.recordlen)*int64(i)
	r.r.Seek(offset, 0)

	var deleted byte
	if err = binary.Read(r.r, binary.LittleEndian, &deleted); err != nil {
		return nil, err
	} else if deleted == '*' {
		return nil, fmt.Errorf("record %d is deleted", i)
	} else if deleted != ' ' {
		return nil, fmt.Errorf("record %d contained an unexpected value in the deleted flag: %h", i, deleted)
	}

	rec = make(Record)
	for i, f := range r.fields {
		buf := make([]byte, f.Len)
		if err = binary.Read(r.r, binary.LittleEndian, &buf); err != nil {
			return nil, err
		}
		fieldVal := strings.TrimSpace(string(buf))
		switch f.Type {
		case 'F':
			rec[r.FieldName(i)], err = strconv.ParseFloat(fieldVal, 64)
		case 'N':
			rec[r.FieldName(i)], err = strconv.Atoi(fieldVal)
		default:
			rec[r.FieldName(i)] = fieldVal
		}
		if err != nil {
			return nil, err
		}
	}
	return rec, nil
}
