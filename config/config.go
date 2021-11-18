package config

import (
	"encoding/json"
	"os"
)

const DefaultConfigPath = "/etc/merklefs/config.json"

type Config struct {
	Pool    string `json:"pool"`
	Remote  string `json:"remote"`
	Fetcher string `json:"fetcher"`
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
