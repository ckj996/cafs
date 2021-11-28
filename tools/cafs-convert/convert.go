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
	"strconv"

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
	pool   string
	zpool  string
	zsize  int64
	zrate  float64
	zlevel string
	bsize  int64
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
		return
	}
}

func (s stasher) zstder() func(n *metadata.Node) {
	return func(n *metadata.Node) {
		if len(n.Value) != 64 { // FIXME
			return
		}
		path := filepath.Join(s.pool, n.Value)
		zpath := filepath.Join(s.zpool, n.Value)
		if _, err := os.Stat(zpath); err == nil {
			os.Remove(path)
			n.Zstd = true
			return
		}

		fi, err := os.Stat(path)
		if err != nil || fi.Size() < s.zsize {
			return
		}

		exec.Command("zstd", s.zlevel, "-o", zpath, path).Run()
		zi, err := os.Stat(zpath)
		if err != nil {
			return
		}

		if float64(zi.Size()) < float64(fi.Size())*s.zrate {
			os.Remove(path)
			n.Zstd = true
		} else {
			os.Remove(zpath)
		}
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
			if cfg.ZLevel == 0 {
				s.zlevel = "-3"
			} else {
				s.zlevel = "-" + strconv.Itoa(cfg.ZLevel)
			}
			s.bsize = cfg.BSize
		}
	} else {
		s.pool = os.Args[3]
	}
	tree := metadata.Tree{}
	if err := tree.Build(root, s.stashTo()); err != nil {
		fmt.Println(err)
	}
	if s.bsize > 0 {
		tree.Bundle(s.bsize, s.pool)
	}
	tree.Walk(s.zstder())
	tree.Save(meta)
}
