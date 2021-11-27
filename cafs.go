//go:build darwin || freebsd || netbsd || openbsd || linux
// +build darwin freebsd netbsd openbsd linux

// Copyright 2021 Kaijie Chen. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package main

import (
	"errors"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/billziss-gh/cgofuse/fuse"
	"github.com/kaijchen/cafs/config"
	"github.com/kaijchen/cafs/location"
	"github.com/kaijchen/cafs/metadata"
	"github.com/kaijchen/cafs/platform"
	"github.com/klauspost/compress/zstd"
)

func errno(err error) int {
	if err != nil {
		return -int(err.(syscall.Errno))
	} else {
		return 0
	}
}

type Cafs struct {
	fuse.FileSystemBase
	metadata.Tree
	pool    string
	remote  string
	fetcher string
	tracker string
	loc     *location.Loc
}

// Init is called when the file system is created.
func (cafs *Cafs) Init() {
}

/*
// Statfs gets file system statistics.
func (cafs *Cafs) Statfs(path string, stat *fuse.Statfs_t) (errc int) {
	path = filepath.Join(cafs.root, path)
	stgo := syscall.Statfs_t{}
	errc = errno(platform.Statfs(path, &stgo))
	platform.CopyFusestatfsFromGostatfs(stat, &stgo)
	return
}
*/

// Readlink reads the target of a symbolic link.
func (cafs *Cafs) Readlink(path string) (errc int, target string) {
	target, errc = cafs.GetLink(path)
	return
}

// Open opens a file.
// The flags are a combination of the fuse.O_* constants.
func (cafs *Cafs) Open(path string, flags int) (errc int, fh uint64) {
	return cafs.open(path, flags, 0)
}

func (cafs *Cafs) get(hash string, zst bool) error {
	tmp := filepath.Join(cafs.pool, "tmp_"+hash)
	if err := cafs.download(hash, tmp, zst); err != nil {
		return err
	}
	object := filepath.Join(cafs.pool, hash)
	err := os.Rename(tmp, object)
	if cafs.loc != nil && err == nil {
		cafs.loc.Report(hash)
	}
	return err
}

func (cafs *Cafs) download(hash, path string, zst bool) error {
	out, err := os.Create(path)
	if err != nil {
		return err
	}
	defer out.Close()

	remote := cafs.remote
	if cafs.loc != nil {
		var t time.Duration
		var tmp string
		for tmp == "" {
			time.Sleep(t * time.Millisecond)
			t += 100
			tmp, _ = cafs.loc.Query(hash)
		}
		remote = tmp
	}
	var resp *http.Response
	if zst {
		url := remote + "zstd/" + hash
		resp, err = http.Get(url)
		if err != nil {
			return err
		}
		if resp.StatusCode != 200 {
			// retry non zst
			zst = false
			resp.Body.Close()
		}
	}
	if !zst {
		url := remote + hash
		resp, err = http.Get(url)
		if err != nil {
			return err
		}
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return errors.New("HTTP Status is not OK")
	}

	if zst {
		var d *zstd.Decoder
		d, err = zstd.NewReader(resp.Body)
		if err != nil {
			return err
		}
		defer d.Close()

		_, err = io.Copy(out, d)
	} else {
		_, err = io.Copy(out, resp.Body)
	}
	return err
}

func (cafs *Cafs) open(path string, flags int, perm uint32) (errc int, fh uint64) {
	var hash string
	var zst bool
	hash, zst, errc = cafs.GetHash(path)
	if errc != 0 {
		fh = ^uint64(0)
		return
	}
	path = filepath.Join(cafs.pool, hash)
	f, e := syscall.Open(path, flags, perm)
	if e == syscall.ENOENT {
		// get object
		if cafs.get(hash, zst) == nil {
			// retry
			f, e = syscall.Open(path, flags, perm)
		}
	}
	if e != nil {
		return errno(e), ^uint64(0)
	}
	return 0, uint64(f)
}

// Getattr gets file attributes.
func (cafs *Cafs) Getattr(path string, stat *fuse.Stat_t, fh uint64) (errc int) {
	stgo := syscall.Stat_t{}
	if fh == ^uint64(0) {
		if cafs.Stat(path, &stgo) != nil {
			return -fuse.ENOENT
		}
	} else {
		errc = errno(syscall.Fstat(int(fh), &stgo))
	}
	platform.CopyFusestatFromGostat(stat, &stgo)
	return
}

// Read reads data from a file.
func (cafs *Cafs) Read(path string, buff []byte, offset int64, fh uint64) (n int) {
	n, e := syscall.Pread(int(fh), buff, offset)
	if e != nil {
		return errno(e)
	}
	return n
}

// Release closes an open file.
func (cafs *Cafs) Release(path string, fh uint64) (errc int) {
	return errno(syscall.Close(int(fh)))
}

/*
func (cafs *Cafs) Opendir(path string) (errc int, fh uint64) {
	// fmt.Println("opendir", path)
	path = filepath.Join(cafs.root, path)
	f, e := syscall.Open(path, syscall.O_RDONLY|syscall.O_DIRECTORY, 0)
	if e != nil {
		return errno(e), ^uint64(0)
	}
	return 0, uint64(f)
}
*/

// Readdir reads a directory.
func (cafs *Cafs) Readdir(path string,
	fill func(name string, stat *fuse.Stat_t, offset int64) bool,
	offset int64,
	fh uint64) (errc int) {

	for _, name := range cafs.ListDir(path) {
		if !fill(name, nil, 0) {
			break
		}
	}
	return 0
}

/*
func (cafs *Cafs) Releasedir(path string, fh uint64) (errc int) {
	return errno(syscall.Close(int(fh)))
}
*/
/*
type config struct {
	Pool    string `json:"pool"`
	Remote  string `json:"remote"`
	Fetcher string `json:"fetcher"`
}

func getConfig(file string) (config, error) {
	cfg := config{}
	data, err := os.ReadFile(file)
	if err != nil {
		return cfg, err
	}
	err = json.Unmarshal(data, &cfg)
	return cfg, err
}
*/

func main() {
	var (
		useFetcher = flag.Bool("fetcher", false, "enable fetcher")
	)
	flag.Parse()
	args := flag.Args()

	cfg, err := config.GetDefaultConfig()

	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	// syscall.Umask(0)
	cafs := Cafs{pool: cfg.Pool, remote: cfg.Remote, tracker: cfg.Tracker}
	if cfg.Tracker != "" {
		loc := location.NewLoc(cfg.Tracker)
		if cfg.Port > 0 {
			loc.SetPort(cfg.Port)
		}
		cafs.loc = &loc
		defer loc.Close()
	}
	if *useFetcher {
		cafs.fetcher = cfg.Fetcher
	}
	cafs.Restore(args[0])
	host := fuse.NewFileSystemHost(&cafs)
	host.Mount("", args[1:])
}
