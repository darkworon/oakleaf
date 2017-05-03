package file

import (
	"github.com/darkworon/oakleaf/part"
	"sync"
)

type File interface {
	AddPart()
}

type unit struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Size int64  `json:"size"`
	//Parts []*Part `json:"Parts"`
	Parts part.List `json:"parts,omitempty"`
}

type List struct {
	sync.RWMutex
	List []*Unit
}

func (file *unit) AddPart(part *part.Unit) *File {
	file.Parts = append(file.Parts, part)
	return file
}
