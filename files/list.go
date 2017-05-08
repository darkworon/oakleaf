package files

import (
	"encoding/json"
	"time"
	//"github.com/darkworon/oakleaf/node"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"oakleaf/cluster"
	"oakleaf/cluster/node/client"
	"oakleaf/config"
	"oakleaf/parts"
	"oakleaf/utils"
	"path/filepath"
	"sync"

	log "github.com/Sirupsen/logrus"
)

type FileListInterface interface {
}

type Files []*File

type List struct {
	sync.RWMutex
	files     Files
	indexLock sync.RWMutex
}

func (fl *List) Add(f *File) {
	_f := <-fl.Find(f.ID)

	if _f == nil {
		fl.Lock()
		fl.files = append(fl.files, f)
		fl.Unlock()
	}
}

func (fl *List) List() []*File {
	var list = []*File{}
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

func All() *List {
	//fmt.Println("HERE")
	//fileList.Lock()
	//defer fileList.Unlock()
	return fileList
}

func (fl *List) Save(dir string) {
	filesJson, _ := json.Marshal(fl.List())
	fl.indexLock.Lock()
	ioutil.WriteFile(filepath.Join(dir, "files.json"), filesJson, 0644)
	fl.indexLock.Unlock()
}

func Save() {
	fileList.Save(config.Get().WorkingDir)
}

func (fl *List) Count() (n int) {
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
		//fc <- nil
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
	return len(f.files), err

}

func LoadFromCluster() (count int, err error) {
	if cluster.Nodes().Count() > 1 {
		n := cluster.AllActive().Except(cluster.CurrentNode()).ToSlice()[0]
		resp, err := client.Get(fmt.Sprintf("%s://%s/files", n.Protocol(), n.Address), 3*time.Second)
		defer resp.Body.Close()
		if err != nil {
			utils.HandleError(err)
		}
		if err == io.ErrClosedPipe {
			// remote is unreachable, need to mark it as unactive
		}
		//fmt.Println(choosenNode.Address)
		err = json.NewDecoder(resp.Body).Decode(&fileList.files)
		if err != nil {
			utils.HandleError(err)
		} else if len(fileList.files) > 0 {
			Save()
		}
	} else {
		err = errors.New("error: no nodes in cluster to load files index list")
		log.Warning("No nodes in cluster, I will try to load index from local file if it exists.")
		fileList.Import(config.Get().WorkingDir, "files.json")
	}
	count = len(fileList.files)
	return count, err
}

var fileList = &List{}
