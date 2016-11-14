package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/tinylib/msgp/msgp"
	"github.com/urfave/cli"
)

func actionShard(c *cli.Context) error {
	return shard(
		c.String("key"),
		c.Int("nbyte"),
	)
}

func shard(shardKeyName string, shardKeyBytes int) (err error) {
	ssw := NewShardsWriter()
	defer func() {
		err2 := ssw.Close()
		if err == nil { // unusual equality intended.
			err = err2
		}
	}()

	buf := new(bytes.Buffer)
	in := bufio.NewReaderSize(os.Stdin, 16*1024)
	dec := msgp.NewReader(io.TeeReader(in, buf))
	decBuf := msgp.NewReaderSize(buf, 4*1024)

	var rows int
	defer func() {
		log.Printf("Processed %d rows", rows)
	}()

	var scratch [16 * 1024]byte
	for {
		if rows%10000 == 0 && rows > 0 {
			log.Printf("Wrote %d rows", rows)
		}

		mh, err := dec.ReadMapHeader()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		// var shardKey string
		var shardWriter *ShardWriter

		for i := uint32(0); i < mh; i++ {
			var k, shardKey []byte
			k, err = dec.ReadMapKeyPtr()
			if err != nil {
				return err
			}

			switch {
			case msgp.UnsafeString(k) == shardKeyName:
				shardKey, err = dec.ReadStringAsBytes(scratch[:])
				if err != nil {
					return err
				}
				slice := shardKeyBytes
				if slice == -1 {
					slice = len(shardKey)
				}
				shardWriter, err = ssw.GetShard(shardKey[:slice])
				if err != nil {
					return err
				}
			default:
				err = dec.Skip()
				if err != nil {
					return err
				}
			}
		}

		if shardWriter == nil {
			err = decBuf.Skip()
			if err != nil {
				return err
			}
		} else {
			_, err = decBuf.CopyNext(shardWriter)
			if err != nil {
				return err
			}
		}

		rows++
	}
}

// ShardsWriter maintains state for a group of column writers
type ShardsWriter struct {
	shards map[string]*ShardWriter
}

// NewShardsWriter initialises a new ShardsWriter
func NewShardsWriter() *ShardsWriter {
	return &ShardsWriter{shards: map[string]*ShardWriter{}}
}

// Close completes all columns and releases related resources.
func (ssw *ShardsWriter) Close() error {
	for _, sw := range ssw.shards {
		err := sw.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// GetShard obtains the writer for the given shard or creates it if needs be.
func (ssw *ShardsWriter) GetShard(c []byte) (*ShardWriter, error) {
	if bytes.Contains(c, []byte("/")) {
		c = bytes.Replace(c, []byte("/"), []byte("_"), -1)
	}
	if cw, ok := ssw.shards[msgp.UnsafeString(c)]; ok {
		return cw, nil
	}
	name := string(c)
	cw, err := ssw.newShardWriter(name)
	if err != nil {
		return nil, err
	}
	ssw.shards[name] = cw
	return cw, nil
}

func (ssw *ShardsWriter) newShardWriter(name string) (*ShardWriter, error) {
	fd, err := os.Create(fmt.Sprintf("s/%s.shard.msg", name))
	if err != nil {
		return nil, err
	}
	return &ShardWriter{
		Writer: bufio.NewWriter(fd),
		Closer: fd,
		name:   name,
	}, err
}

// ShardWriter is responsible for writing a single column.
type ShardWriter struct {
	*bufio.Writer
	io.Closer
	name   string
	writes uint64
}

// Close and flush the column.
func (sw *ShardWriter) Close() error {
	err1 := sw.Flush()
	err2 := sw.Closer.Close()
	if err1 != nil {
		return err1
	}
	return err2
}
