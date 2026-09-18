package config

import (
	"fmt"
	"os"
)

func requiredEnv(name string) (string, error) {
	value := os.Getenv(name)
	if value == "" {
		return "", fmt.Errorf("%s is not set", name)
	}

	return value, nil
}
