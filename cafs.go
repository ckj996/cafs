//go:build darwin || freebsd || netbsd || openbsd || linux
// +build darwin freebsd netbsd openbsd linux

// Copyright 2021 Kaijie Chen. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"syscall"

	"github.com/billziss-gh/cgofuse/fuse"
	"github.com/kaijchen/cafs/config"
	"github.com/kaijchen/cafs/metadata"
	"github.com/kaijchen/cafs/platform"
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

func (cafs *Cafs) get(hash string) error {
	tmp := filepath.Join(cafs.pool, "tmp_"+hash)
	if err := cafs.download(hash, tmp); err != nil {
		return err
	}
	object := filepath.Join(cafs.pool, hash)
	return os.Rename(tmp, object)
}

func (cafs *Cafs) download(hash, path string) error {
	out, err := os.Create(path)
	if err != nil {
		return err
	}
	defer out.Close()

	resp, err := http.Get(cafs.remote + hash)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}

func (cafs *Cafs) open(path string, flags int, perm uint32) (errc int, fh uint64) {
	var hash string
	hash, errc = cafs.GetHash(path)
	if errc != 0 {
		fh = ^uint64(0)
		return
	}
	path = filepath.Join(cafs.pool, hash)
	f, e := syscall.Open(path, flags, perm)
	if e == syscall.ENOENT {
		// get object
		if cafs.get(hash) == nil {
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
	cafs := Cafs{pool: cfg.Pool, remote: cfg.Remote}
	if *useFetcher {
		cafs.fetcher = cfg.Fetcher
	}
	cafs.Restore(args[0])
	host := fuse.NewFileSystemHost(&cafs)
	host.Mount("", args[1:])
}
