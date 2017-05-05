package file

import (
	"encoding/json"
	//"github.com/darkworon/oakleaf/cluster"
	//"github.com/darkworon/oakleaf/node"
	"oakleaf/part"
)

type PartsList []*part.Part
type PartUploadOptions part.PartUploadOptions

//type Node *node.Node

//type Nodes *cluster.NodesList

type FileInterface interface {
	AddPart() *File
	IsAvailable() bool
	ToJson() []byte
}

type File struct {
	FileInterface `json:"-"`
	ID            string `json:"id"`
	Name          string `json:"name"`
	Size          int64  `json:"size"`
	//Parts []*Part `json:"Parts"`
	Parts PartsList `json:"parts,omitempty"`
}

func (f *File) AddPart(part *part.Part) {
	f.Parts = append(f.Parts, part)
	//return file
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
	Parts       omit   `json:"parts,omitempty"`
	DownloadURL string `json:"download_url"`
}

type omit *struct{}

func FromJson(data []byte) *File {
	return nil
}
