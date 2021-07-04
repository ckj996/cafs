package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/ckj996/cafs/metadata"
	flags "github.com/jessevdk/go-flags"
)

var opts struct {
	Verbose []bool `short:"v" long:"verbose" description:"Show verbose debug information"`
	From    string `short:"f" long:"from" description:"Source directory to convert from" required:"true"`
	Out     string `short:"o" long:"output" description:"File to store metadata" required:"true"`
	Pool    string `short:"p" long:"pool" description:"Content-addressable storage pool"`
}

func sha256sum(path string) (checksum string) {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		log.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func stash(path string) (checksum string) {
	checksum = sha256sum(path)
	if opts.Pool == "" {
		return
	}
	caspath := filepath.Join(opts.Pool, checksum)
	if _, err := os.Stat(caspath); os.IsNotExist(err) {
		os.Link(path, filepath.Join(opts.Pool, checksum))
	}
	return
}

func main() {
	if _, err := flags.Parse(&opts); err != nil {
		os.Exit(-1)
	}
	tree := metadata.Tree{}
	if err := tree.Build(opts.From, stash); err != nil {
		fmt.Println(err)
	}
	tree.Save(opts.Out)
}
