package config

import (
	"encoding/json"
	"os"
)

const DefaultConfigPath = "/etc/merklefs/config.json"

type Config struct {
	Pool    string  `json:"pool"`
	Zpool   string  `json:"zpool"`
	Tpool   string  `json:"tpool"`
	Remote  string  `json:"remote"`
	Port    int     `json:"port"`
	Fetcher string  `json:"fetcher"`
	Tracker string  `json:"tracker"`
	ZSize   int64   `json:"zsize"`
	ZRate   float64 `json:"zrate"`
	ZLevel  int     `json:"zlevel"`
	BSize   int64   `json:"bsize"`
	BRefs   string  `json:"brefs"`
	ASize   int64   `json:"asize"`
}

func (cfg *Config) Load(file string) error {
	data, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, cfg)
}

func GetConfig(file string) (cfg Config, err error) {
	err = cfg.Load(file)
	return
}

func GetDefaultConfig() (cfg Config, err error) {
	return GetConfig(DefaultConfigPath)
}
