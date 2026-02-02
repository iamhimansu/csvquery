package query

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

type Schema struct {
	VirtualColumns map[string]string `json:"virtual_columns"`
	path           string
	mu             sync.Mutex
}

func LoadSchema(csvPath string) (*Schema, error) {
	s := &Schema{
		VirtualColumns: make(map[string]string),
		path:           getSchemaPath(csvPath),
	}
	if _, err := os.Stat(s.path); os.IsNotExist(err) {
		return s, nil
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, s); err != nil {
		return nil, err
	}
	if s.VirtualColumns == nil {
		s.VirtualColumns = make(map[string]string)
	}
	return s, nil
}

func (s *Schema) Save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, data, 0644)
}

func getSchemaPath(csvPath string) string {
	dir := filepath.Dir(csvPath)
	base := filepath.Base(csvPath)
	return filepath.Join(dir, base+"_schema.json")
}
