package metadata

import (
	"encoding/json"
	"errors"
	"io/fs"
	"log"
	"os"
	"path/filepath"
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
			hash := toValue(path)
			if strings.HasSuffix(hash, ".zst") {
				node.Zstd = true
				node.Value = hash[:len(hash)-4]
			} else {
				node.Value = hash
			}
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

type Node struct {
	Ino     uint64            `json:"ino"`
	Mode    uint32            `json:"mode"`
	Size    int64             `json:"size"`
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
