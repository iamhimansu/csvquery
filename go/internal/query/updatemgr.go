package query

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type UpdateManager struct {
	csvPath    string
	schemaPath string
	mu         sync.RWMutex
	Overrides  map[string]map[string]string `json:"rows"`
}

func LoadUpdates(csvPath string) (*UpdateManager, error) {
	absPath, err := filepath.Abs(csvPath)
	if err != nil {
		return nil, err
	}
	schemaPath := absPath + "_updates.json"
	um := &UpdateManager{
		csvPath:    absPath,
		schemaPath: schemaPath,
		Overrides:  make(map[string]map[string]string),
	}
	if _, err := os.Stat(schemaPath); err == nil {
		data, err := os.ReadFile(schemaPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read updates file: %v", err)
		}
		if len(data) > 0 {
			if err := json.Unmarshal(data, um); err != nil {
				return nil, fmt.Errorf("failed to parse updates file: %v", err)
			}
		}
	}
	return um, nil
}

func (um *UpdateManager) Save() error {
	um.mu.RLock()
	defer um.mu.RUnlock()
	data, err := json.MarshalIndent(um, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(um.schemaPath, data, 0644)
}

func (um *UpdateManager) GetRow(offset int64) map[string]string {
	um.mu.RLock()
	defer um.mu.RUnlock()
	key := fmt.Sprintf("%d", offset)
	if row, ok := um.Overrides[key]; ok {
		return row
	}
	return nil
}
