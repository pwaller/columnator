package main

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/tinylib/msgp/msgp"
)

// ColumnsWriter maintains state for a group of column writers
type ColumnsWriter struct {
	rows uint64
	cs   map[string]*ColumnWriter
}

// NewColumnsWriter initialises a new ColumnsWriter
func NewColumnsWriter() *ColumnsWriter {
	return &ColumnsWriter{cs: map[string]*ColumnWriter{}}
}

// NextRow updates the internal state of the ColumnWriter.
func (csw *ColumnsWriter) NextRow() {
	csw.rows++
}

// Close completes all columns and releases related resources.
func (csw *ColumnsWriter) Close() error {
	for _, cw := range csw.cs {
		err := cw.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// GetColumn obtains the writer for the given column or creates it if needs be.
// It is safe to pass a column name c which will be modified in the future to
// avoid allocations.
func (csw *ColumnsWriter) GetColumn(c []byte) (*ColumnWriter, error) {
	if cw, ok := csw.cs[msgp.UnsafeString(c)]; ok {
		return cw, nil
	}
	name := string(c)
	cw, err := csw.newColumnWriter(name)
	if err != nil {
		return nil, err
	}
	csw.cs[name] = cw
	return cw, nil
}

func (csw *ColumnsWriter) newColumnWriter(name string) (*ColumnWriter, error) {
	fd, err := os.Create(fmt.Sprintf("d/test_%s.col.msg", name))
	if err != nil {
		return nil, err
	}
	return &ColumnWriter{
		Writer: bufio.NewWriter(fd),
		Closer: fd,
		parent: csw,
		name:   name,
	}, err
}

// ColumnWriter is responsible for writing a single column.
type ColumnWriter struct {
	*bufio.Writer
	io.Closer
	parent   *ColumnsWriter
	position uint64
	name     string
	writes   uint64
}

// catchUp writes the correct number of nils into the stream so that the
// resulting array is square.
func (cw *ColumnWriter) catchUp() error {
	n := cw.parent.rows - cw.position
	for i := uint64(0); i < n; i++ {
		cw.writes++
		nilByte := []byte{0xc0} // msgpack "nil"
		_, err := cw.Writer.Write(nilByte)
		if err != nil {
			return err
		}
	}
	cw.position += n
	return nil
}

// Write bytes to the column.
func (cw *ColumnWriter) Write(p []byte) (int, error) {
	if cw.position < cw.parent.rows {
		err := cw.catchUp()
		if err != nil {
			return 0, err
		}
	}
	cw.position++
	cw.writes++
	return cw.Writer.Write(p)
}

// Close and flush the column.
func (cw *ColumnWriter) Close() error {
	// Check we're caught up with the parent ColumnsWriter.
	err := cw.catchUp()
	if err != nil {
		return err
	}
	if cw.writes != cw.parent.rows {
		return fmt.Errorf("wrong number of rows written: %d != %d",
			cw.writes, cw.parent.rows)
	}
	err1 := cw.Flush()
	err2 := cw.Closer.Close()
	if err1 != nil {
		return err1
	}
	return err2
}
