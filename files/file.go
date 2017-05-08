package files

import (
	"encoding/json"
	//"github.com/darkworon/oakleaf/cluster"
	//"github.com/darkworon/oakleaf/node"
	"oakleaf/parts"
)

type PartsList []*parts.Part

type FileInterface interface {
}

type File struct {
	FileInterface `json:"-"`
	ID            string `json:"id"`
	Name          string `json:"name"`
	Size          int64  `json:"size"`
	//Parts []*Part `json:"Parts"`
	Parts PartsList `json:"parts,omitempty"`
}

func (f *File) AddPart(part *parts.Part) {
	f.Parts = append(f.Parts, part)
	//return files
}

func (f *File) IsAvailable() bool {
	for _, z := range f.Parts {
		if !z.IsAvailable() {
			return false
		}
	}
	return true
}

func (f *File) ToJson() []byte {
	a, _ := json.Marshal(f)
	return a
}

type PublicFile struct {
	*File
	ID           omit   `json:"-"`
	Parts        omit   `json:"parts,omitempty"`
	DownloadURL  string `json:"url"`
	DeleteURL    string `json:"deleteUrl"`
	DeleteMethod string `json:"deleteType"`
}

type PublicFiles struct {
	Files []PublicFile `json:"files"`
}

type omit *struct{}

func FromJson(data []byte) *File {
	return nil
}
