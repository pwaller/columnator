package main

import (
	"log"
	"os"

	"github.com/urfave/cli"
)

func main() {
	cli.NewApp()
	app := cli.NewApp()

	app.Commands = []cli.Command{
		{
			Name:    "transpose",
			Aliases: []string{"t"},
			Usage:   "transpose dictionaries into separate files",
			Action:  actionTranspose,
		},
		{
			Name:    "shard",
			Aliases: []string{"s"},
			Usage:   "shard dictionaries",
			Action:  actionShard,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "key",
					Usage: "variable to shard over",
					Value: "distinct_id",
				},
				cli.IntFlag{
					Name: "nbyte",
					Usage: "number of bytes on the left " +
						"of the value to use as the " +
						"shard name (-1 means 'all')",
					Value: 4,
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
