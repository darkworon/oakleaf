package files

import (
	"encoding/json"
	//"github.com/darkworon/oakleaf/node"
	"io/ioutil"
	"oakleaf/parts"
	"oakleaf/utils"
	"path/filepath"
	"sync"
)

type FileListInterface interface {

}

type Files []*File

type List struct {
	sync.RWMutex
	indexLock sync.RWMutex
	files     Files
}

func (fl *List) Add(f *File) {
	_f := <-fl.Find(f.ID)
	fl.Lock()
	if _f == nil {
		fl.files = append(fl.files, f)
		//go fl.Save()
	}
	defer fl.Unlock()
}

func (fl *List) List() (list []*File) {
	fl.Lock()
	list = append(list, fl.files...)
	fl.Unlock()
	return list
}

func (fl *List) All() (fl2 *List) {
	fl.Lock()
	fl2.files = append(fl2.files, fl.files...)
	fl.Unlock()
	return fl2
}

func (fl *List) Save(dir string) {
	fl.Lock()
	filesJson, _ := json.Marshal(fl.files)
	fl.Unlock()
	fl.indexLock.Lock()
	defer fl.indexLock.Unlock()
	ioutil.WriteFile(filepath.Join(dir, "files.json"), filesJson, 0644)
}

func (fl *List) Count() (n int) {
	fl.Lock()
	defer fl.Unlock()
	return len(fl.files)
}

func (fl *List) Find(value string) <-chan *File {
	fc := make(chan *File)
	f := func() {
		fl.Lock()
		defer fl.Unlock()
		for _, v := range fl.files {
			if v.ID == value {
				fc <- v
			}
		}
		close(fc)
	}
	go f()

	return fc
}

func (f *List) FindPart(value string) *parts.Part {
	for _, v := range f.files {
		for _, z := range v.Parts {
			if z.ID == value {
				return z
			}
		}
	}
	return nil
}

func (f *List) ToJson() []byte {
	a, _ := json.Marshal(f.files)
	return a
}

func (f *List) Import(dir, name string) (int, error) {
	var _filesJson, err = utils.LoadExistingFile(filepath.Join(dir, name))
	err = json.Unmarshal(_filesJson, &f.files)
	if err != nil {
		utils.HandleError(err)
	}
	return len(f.files), err

}

var FileList = List{}
