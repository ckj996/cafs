package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/kaijchen/cafs/config"
	"github.com/kaijchen/cafs/metadata"
)

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

type stasher struct {
	pool  string
	zpool string
	zsize int64
	zrate float64
}

func (s stasher) stashTo() func(path string) string {
	if s.pool == "" {
		return sha256sum
	}
	return func(path string) (checksum string) {
		checksum = sha256sum(path)
		caspath := filepath.Join(s.pool, checksum)
		if _, err := os.Stat(caspath); os.IsNotExist(err) {
			os.Link(path, caspath)
		}
		if fi, err := os.Stat(caspath); err == nil {
			if fi.Size() < s.zsize {
				return
			}
			zpath := filepath.Join(s.zpool, checksum)
			if _, err := os.Stat(zpath); os.IsNotExist(err) {
				exec.Command("zstd", "-o", zpath, path).Run()
			}
			zi, err := os.Stat(zpath)
			if err != nil {
				return
			}
			if float64(zi.Size()) < float64(fi.Size())*s.zrate {
				os.Remove(caspath)
				checksum = checksum + ".zst"
			}
		}
		return
	}
}

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("Usage: %v root meta [pool]\n", os.Args[0])
		os.Exit(-1)
	}
	root, meta := os.Args[1], os.Args[2]
	var s stasher
	if len(os.Args) < 4 {
		if cfg, err := config.GetDefaultConfig(); err == nil {
			s.pool = cfg.Pool
			s.zpool = cfg.Zpool
			s.zsize = cfg.ZSize
			s.zrate = cfg.ZRate
		}
	} else {
		s.pool = os.Args[3]
	}
	tree := metadata.Tree{}
	if err := tree.Build(root, s.stashTo()); err != nil {
		fmt.Println(err)
	}
	tree.Save(meta)
}
