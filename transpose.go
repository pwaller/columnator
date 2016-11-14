package main

import (
	"io"
	"log"
	"os"

	"github.com/tinylib/msgp/msgp"
	"github.com/urfave/cli"
)

func actionTranspose(c *cli.Context) error {
	return transposeDictionaries()
}

func transposeDictionaries() (err error) {
	dec := msgp.NewReader(os.Stdin)

	cw := NewColumnsWriter()
	defer func() {
		err2 := cw.Close()
		if err == nil {
			err = err2
		}
	}()

	var r int
	defer func() { log.Printf("Read %d records", r) }()
	for {
		if r%10000 == 0 {
			log.Printf("Wrote %d rows", r)
		}

		mh, err := dec.ReadMapHeader()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		for i := uint32(0); i < mh; i++ {
			k, err := dec.ReadMapKeyPtr()
			if err != nil {
				return err
			}

			c, err := cw.GetColumn(k)
			if err != nil {
				return err
			}

			t, err := dec.NextType()
			if err != nil {
				return err
			}

			_, err = dec.CopyNext(c)
			if err != nil {
				return err
			}
			_ = t
		}

		cw.NextRow()
		r++
	}
}
