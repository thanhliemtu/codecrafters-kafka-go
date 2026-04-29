package main

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func dumpClusterMetadataLog(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to read cluster metadata log: %v\n", err)
		return
	}

	encoded := base64.StdEncoding.EncodeToString(data)

	fmt.Fprintln(os.Stderr, "----- BEGIN __cluster_metadata LOG BASE64 -----")

	// Print in chunks so terminals/log viewers don't hate one giant line.
	const width = 76
	for i := 0; i < len(encoded); i += width {
		end := i + width
		if end > len(encoded) {
			end = len(encoded)
		}
		fmt.Fprintln(os.Stderr, encoded[i:end])
	}

	fmt.Fprintln(os.Stderr, "----- END __cluster_metadata LOG BASE64 -----")
}

func readServerPropertiesFile(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	props := make(map[string]string)

	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)

		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}

		props[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}

	return props, nil
}

func firstLogDir(logDirs string) string {
	if logDirs == "" {
		return ""
	}

	first, _, _ := strings.Cut(logDirs, ",")
	return strings.TrimSpace(first)
}

func getMetadataLogFilePath(logDir string) string {
	return filepath.Join(
		logDir,
		"__cluster_metadata-0",
		"00000000000000000000.log",
	)
}
