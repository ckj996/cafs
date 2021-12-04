package metadata

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
)

type Tree struct {
	nodes []Node
}

func (t *Tree) NewNode(stat *syscall.Stat_t) *Node {
	i := len(t.nodes)
	t.nodes = append(t.nodes, Node{
		Ino:  uint64(i) + 1,
		Mode: stat.Mode,
		Size: stat.Size,
	})
	return &t.nodes[i]
}

func (t *Tree) Dump() ([]byte, error) {
	return json.Marshal(t.nodes)
}

func (t *Tree) Load(data []byte) error {
	return json.Unmarshal(data, &t.nodes)
}

func (t *Tree) Save(filename string) error {
	data, err := t.Dump()
	if err != nil {
		return err
	}
	return os.WriteFile(filename, data, os.FileMode(0644))
}

func (t *Tree) Restore(filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	return t.Load(data)
}

func (t *Tree) Build(root string, toValue func(path string) string) error {
	if oldwd, err := os.Getwd(); err != nil {
		return err
	} else {
		defer os.Chdir(oldwd)
	}
	if err := os.Chdir(root); err != nil {
		return err
	}
	*t = Tree{}
	return filepath.Walk(".", func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		node := t.NewNode(info.Sys().(*syscall.Stat_t))
		if info.IsDir() {
			node.Dirents = make(map[string]uint64)
			node.Dirents["."] = node.Ino
		} else if info.Mode().IsRegular() {
			node.Value = toValue(path)
		} else if info.Mode()&fs.ModeSymlink != 0 {
			node.Value, _ = os.Readlink(path)
		} else {
			log.Printf("[WARN] %q: unexpected file type", path)
		}
		if path != "." {
			dir, file := filepath.Split(path)
			parent := t.lookup(dir)
			parent.Dirents[file] = node.Ino
			if node.IsDir() {
				node.Dirents[".."] = parent.Ino
			}
		} else {
			node.Dirents[".."] = node.Ino
		}
		return nil
	})
}

func (t *Tree) Walk(op func(n *Node)) {
	for i := range t.nodes {
		op(&t.nodes[i])
	}
}

type sino struct {
	size int64
	ino  uint64
}

type sinos []sino

func (s sinos) Len() int { return len(s) }
func (s sinos) Less(i, j int) bool {
	return s[i].size == s[j].size && s[i].ino < s[j].ino || s[i].size < s[j].size
}
func (s sinos) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

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

type Bref struct {
	Hash string `json:"hash"`
	Off  int64  `json:"off"`
}

func (b Bref) Dump() ([]byte, error) {
	return json.Marshal(b)
}

func (b *Bref) Load(data []byte) error {
	return json.Unmarshal(data, b)
}

func (b Bref) Save(filename string) error {
	data, err := b.Dump()
	if err != nil {
		return err
	}
	return os.WriteFile(filename, data, os.FileMode(0644))
}

func (b Bref) Restore(filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	return b.Load(data)
}

func (t *Tree) Bundle(bsize int64, asize int64, pool string, brefs string) {
	for i := range t.nodes {
		n := &t.nodes[i]
		if len(n.Dirents) == 0 {
			continue
		}
		var files sinos
		for _, ino := range n.Dirents {
			ino -= 1 // NOTE: t.nodes[0].Ino == 1
			n := &t.nodes[ino]
			if len(n.Value) == 64 && n.Size < bsize {
				files = append(files, sino{size: t.nodes[ino].Size, ino: ino})
			}
		}
		// filter files
		if brefs != "" {
			j := 0
			ref := Bref{}
			for _, f := range files {
				fi := &t.nodes[f.ino]
				rpath := filepath.Join(brefs, fi.Value)
				if _, err := os.Stat(rpath); err == nil {
					ref.Restore(rpath)
					fi.Value = ref.Hash
					fi.Off = ref.Off
				} else {
					files[j] = f
					j++
				}
			}
			files = files[:j]
		}
		if len(files) < 2 {
			if brefs != "" && len(files) == 1 {
				fi := &t.nodes[files[0].ino]
				ref := Bref{Hash: fi.Value, Off: 0}
				ref.Save(filepath.Join(brefs, ref.Hash))
			}
			continue
		}
		sort.Sort(files)
		var pending []uint64
		var off int64
		var tmp *os.File
		tpath := filepath.Join(pool, "_bundle")
		for _, f := range files {
			fi := &t.nodes[f.ino]
			if off == 0 {
				tmp, _ = os.Create(tpath)
			}
			fp, _ := os.Open(filepath.Join(pool, fi.Value))
			io.Copy(tmp, fp)
			fp.Close()
			fi.Off = off
			pending = append(pending, f.ino)
			off += fi.Size
			if asize > 0 {
				off = ((off-1)/asize + 1) * asize
				tmp.Seek(off, 0)
			}
			if off >= bsize {
				tmp.Close()
				hash := sha256sum(tpath)
				os.Rename(tpath, filepath.Join(pool, hash))
				for _, ino := range pending {
					ref := Bref{Hash: hash, Off: t.nodes[ino].Off}
					ref.Save(filepath.Join(brefs, t.nodes[ino].Value))
					t.nodes[ino].Value = hash
				}
				off = 0
				pending = pending[:0]
			}
		}
		if off > 0 {
			tmp.Close()
			hash := sha256sum(tpath)
			os.Rename(tpath, filepath.Join(pool, hash))
			for _, ino := range pending {
				t.nodes[ino].Value = hash
			}
		}
	}
}

func (t *Tree) lookup(path string) *Node {
	path = filepath.Clean(path)
	i := 0
	for _, name := range strings.Split(path, string(filepath.Separator)) {
		if name == "" {
			continue
		}
		ino := t.nodes[i].Dirents[name]
		if ino == 0 {
			return nil
		}
		i = int(ino - 1)
	}
	return &t.nodes[i]
}

func (t *Tree) ListDir(path string) (names []string) {
	dir := t.lookup(path)
	if dir == nil {
		return
	}
	for name := range dir.Dirents {
		names = append(names, name)
	}
	return
}

func (t *Tree) Stat(path string, stat *syscall.Stat_t) error {
	file := t.lookup(path)
	if file == nil {
		return errors.New("file not exist")
	}
	file.Stat(stat)
	stat.Nlink = 1
	stat.Blksize = 4096
	stat.Blocks = stat.Size / stat.Blksize
	if stat.Size%stat.Blksize > 0 {
		stat.Blocks++
	}
	return nil
}

func (t *Tree) GetLink(path string) (lnk string, errc int) {
	file := t.lookup(path)
	if file == nil {
		errc = -int(syscall.ENOENT)
		return
	}
	if !file.IsLnk() {
		errc = -int(syscall.EINVAL)
		return
	}
	lnk = file.Value
	return
}

func (t *Tree) GetHash(path string) (hash string, zstd bool, errc int) {
	file := t.lookup(path)
	if file == nil {
		errc = -int(syscall.ENOENT)
		return
	}
	if !file.IsReg() {
		errc = -int(syscall.EINVAL)
		return
	}
	hash = file.Value
	zstd = file.Zstd
	return
}

func (t *Tree) GetOff(path string) int64 {
	file := t.lookup(path)
	if file == nil {
		return 0
	}
	return file.Off
}

type Node struct {
	Ino     uint64            `json:"ino"`
	Mode    uint32            `json:"mode"`
	Size    int64             `json:"size"`
	Off     int64             `json:"off,omitempty"`
	Value   string            `json:"value,omitempty"`
	Zstd    bool              `json:"zstd,omitempty"`
	Dirents map[string]uint64 `json:"dirents,omitempty"`
}

func (n *Node) IsDir() bool {
	return n.Mode&syscall.S_IFDIR != 0
}

func (n *Node) IsLnk() bool {
	return n.Mode&syscall.S_IFLNK != 0
}

func (n *Node) IsReg() bool {
	return n.Mode&syscall.S_IFREG != 0
}

func (n *Node) Stat(stat *syscall.Stat_t) {
	stat.Ino = n.Ino
	stat.Mode = n.Mode
	stat.Size = n.Size
}
