// +build darwin freebsd netbsd openbsd linux

// Copyright 2021 Kaijie Chen. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package main

import (
	"os"
	"path/filepath"
	"syscall"

	"github.com/billziss-gh/cgofuse/fuse"
	"github.com/ckj996/cafs/metadata"
	"github.com/ckj996/cafs/platform"
	"github.com/jessevdk/go-flags"
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
	pool string
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

func (cafs *Cafs) open(path string, flags int, perm uint32) (errc int, fh uint64) {
	var hash string
	hash, errc = cafs.GetHash(path)
	if errc != 0 {
		fh = ^uint64(0)
		return
	}
	path = filepath.Join(cafs.pool, hash)
	f, e := syscall.Open(path, flags, perm)
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

func main() {

	var opts struct {
		Verbose []bool   `short:"v" long:"verbose" description:"Show verbose debug information"`
		Mnt     string   `short:"t" long:"target" description:"Metadata file" required:"true"`
		Meta    string   `short:"s" long:"source" description:"Metadata file" required:"true"`
		Opts    []string `short:"o" long:"option" description:"FUSE mount options"`
		Pool    string   `short:"p" long:"pool" description:"Content-addressable storage pool" required:"true"`
	}

	if _, err := flags.Parse(&opts); err != nil {
		os.Exit(-1)
	}
	// syscall.Umask(0)
	cafs := Cafs{}
	cafs.Restore(opts.Meta)
	cafs.pool = opts.Pool
	args := make([]string, 0, len(opts.Opts)*2+1)
	for _, op := range opts.Opts {
		args = append(args, "-o", op)
	}
	args = append(args, opts.Mnt)
	host := fuse.NewFileSystemHost(&cafs)
	host.Mount("", args)
}
