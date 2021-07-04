// +build openbsd

/*
 * port_openbsd.go
 *
 * Copyright 2017-2020 Bill Zissimopoulos
 */
/*
 * This file is part of Cgofuse.
 *
 * It is licensed under the MIT license. The full license text can be found
 * in the License.txt file at the root of this project.
 */

package platform

import (
	"syscall"

	"github.com/billziss-gh/cgofuse/fuse"
)

func Setuidgid() func() {
	euid := syscall.Geteuid()
	if 0 == euid {
		uid, gid, _ := fuse.Getcontext()
		egid := syscall.Getegid()
		syscall.Setegid(int(gid))
		syscall.Seteuid(int(uid))
		return func() {
			syscall.Seteuid(euid)
			syscall.Setegid(egid)
		}
	}
	return func() {
	}
}

func CopyFusestatfsFromGostatfs(dst *fuse.Statfs_t, src *syscall.Statfs_t) {
	*dst = fuse.Statfs_t{}
	dst.Bsize = uint64(src.F_bsize)
	dst.Frsize = 1
	dst.Blocks = uint64(src.F_blocks)
	dst.Bfree = uint64(src.F_bfree)
	dst.Bavail = uint64(src.F_bavail)
	dst.Files = uint64(src.F_files)
	dst.Ffree = uint64(src.F_ffree)
	dst.Favail = uint64(src.F_ffree)
	dst.Namemax = 255 //uint64(src.Namelen)
}

func CopyFusestatFromGostat(dst *fuse.Stat_t, src *syscall.Stat_t) {
	*dst = fuse.Stat_t{}
	dst.Dev = uint64(src.Dev)
	dst.Ino = uint64(src.Ino)
	dst.Mode = uint32(src.Mode)
	dst.Nlink = uint32(src.Nlink)
	dst.Uid = uint32(src.Uid)
	dst.Gid = uint32(src.Gid)
	dst.Rdev = uint64(src.Rdev)
	dst.Size = int64(src.Size)
	dst.Atim.Sec, dst.Atim.Nsec = src.Atim.Sec, src.Atim.Nsec
	dst.Mtim.Sec, dst.Mtim.Nsec = src.Mtim.Sec, src.Mtim.Nsec
	dst.Ctim.Sec, dst.Ctim.Nsec = src.Ctim.Sec, src.Ctim.Nsec
	dst.Blksize = int64(src.Blksize)
	dst.Blocks = int64(src.Blocks)
	dst.Birthtim.Sec, dst.Birthtim.Nsec = src.X__st_birthtim.Sec, src.X__st_birthtim.Nsec
}

func syscall_Statfs(path string, stat *syscall.Statfs_t) error {
	return syscall.Statfs(path, stat)
}
