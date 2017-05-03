package node

import (
	//"sync"
	"time"
)

type Unit struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	Address    string    `json:"address"`
	IsActive   bool      `json:"is_active"`
	TotalSpace int64     `json:"total_space"`
	UsedSpace  int64     `json:"used_space"`
	FilesCount int       `json:"files_count"`
	PartsCount int       `json:"parts_count"`
	LastUpdate time.Time `json:"last_update"`
	//Parts      []string  `json:"parts,omitempty"`
}
