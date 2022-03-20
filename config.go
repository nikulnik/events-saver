package main

import "os"

type Config struct {
	ClickhouseHost     string
	ClickhousePort     string
	ClickhouseDatabase string
}

func NewConfig() Config {
	return Config{
		ClickhouseHost:     GetOrDefault("CH_HOST", "clickhouse-server"),
		ClickhousePort:     GetOrDefault("CH_POST", "9000"),
		ClickhouseDatabase: GetOrDefault("CH_DB", "events"),
	}
}

func GetOrDefault(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		value = defaultValue
	}
	return value
}
