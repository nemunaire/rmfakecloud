package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/ddvk/rmfakecloud/internal/common"
)

func main() {
	flag.Parse()

	var reader io.Reader
	if flag.NArg() > 0 {
		var err error
		reader, err = os.Open(flag.Arg(0))
		if err != nil {
			log.Fatal("Unable to open file:", err.Error())
		}
		defer reader.(*os.File).Close()
	} else {
		reader = os.Stdin
	}

	crc, err := common.CRC32FromReader(reader)
	if err != nil {
		log.Fatal("Unable to compute checksum:", err.Error())
	}
	fmt.Println("crc32c=" + crc)
}
