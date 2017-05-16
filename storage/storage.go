package storage

import (
	//"oakleaf/cluster"
	//	"io/ioutil"
	"oakleaf/config"
	"oakleaf/utils"
	//	"os"
	"path/filepath"
	//	"sort"
	"encoding/json"
	"io/ioutil"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/satori/go.uuid"
)

type Parts []*Part

type Part struct {
	ID        string    `json:"id"`
	Path      string    `json:"path"`
	Size      int64     `json:"size"`
	CreatedAt time.Time `json:"created_at"`
}

type Listing struct {
	indexLock    sync.RWMutex `json:"-"`
	sync.RWMutex `json:"-"`
	Parts        Parts `json:"parts"`
}

type PartChan <-chan Parts

//type File *files.File
func PartsCount(c *config.Config) int {
	return utils.GetFilesCount(filepath.Join(c.WorkingDir, c.ConfigFile))

}

func NewUUID() string {
	return uuid.NewV4().String()
}

func GetDirectory(id string) string {
	//sa := strings.Split(id, "-")
	sa := strings.Replace(id, "-", "", -1)
	l1 := sa[:2]
	l2 := sa[2:4]
	return filepath.Join(config.Get().DataDir, l1, l2)
}

func GetFullPath(id string) string {
	sa := strings.Replace(id, "-", "", -1)
	l1 := sa[:2]
	l2 := sa[2:4]
	f := sa[4:]
	return filepath.Join(config.Get().DataDir, l1, l2, f)
}

func GetURI(id string) string {
	sa := strings.Replace(id, "-", "", -1)
	l1 := sa[:2]
	l2 := sa[2:4]
	f := sa[4:]
	return filepath.Join(l1, l2, f)
}

//func (s *Listing) Slice() Infos {
//
//}

/*
func NewPath() *Path {

}*/

func All() PartChan {
	pc := make(chan Parts)
	go func() {
		list.Lock()
		defer list.Unlock()
		l := (<-New()).Parts
		l = append(l, list.Parts...)
		pc <- l
		close(pc)
	}()
	return pc
}

func (l PartChan) Sort() Parts {
	a := <-l
	sort.Slice(a, func(i, j int) bool {
		return a[i].Size < a[j].Size
	})
	return a
}

func Add(p *Part) {
	list.Lock()
	list.Parts = append(list.Parts, p)
	list.Unlock()
	go Save()
}

func Delete(p *Part) {
	list.Lock()
	a := list.Parts
	for i, x := range a {
		if x.ID == p.ID {
			if i == 0 {
				a = a[1:]
			} else if i == len(a) {
				a = a[:len(a)-1]
			} else {
				a[i] = a[len(a)-1]
				a = a[:len(a)-1]
			}
			list.Parts = a
		}
	}
	list.Unlock()
	go Save()
}

func New() <-chan *Listing {
	pc := make(chan *Listing)
	go func() {
		defer close(pc)
		pc <- &Listing{}
	}()
	return pc
}

func (l PartChan) Json() []byte {
	//list.Lock()
	//defer list.Unlock()
	listJson, _ := json.Marshal(<-l)
	return listJson
}

func Count() int {
	return len(list.Parts)
}

func Save() {
	data := All().Json()
	//fmt.Println("Saving...")
	//list.indexLock.Lock()
	ioutil.WriteFile(filepath.Join(config.Get().WorkingDir, "parts.json"), data, 0644)
	//list.indexLock.Unlock()
	//fmt.Println("Done!")

}

func Import(dir, name string) (int, error) {
	list.Lock()
	list.indexLock.Lock()
	defer list.Unlock()
	defer list.indexLock.Unlock()
	a := list.Parts
	var _json, err = utils.LoadExistingFile(filepath.Join(dir, name))
	err = json.Unmarshal(_json, &a)
	if err != nil {
		utils.HandleError(err)
	}
	list.Parts = a
	return Count(), err

}

func Find(id string) <-chan *Part {
	a := list
	pc := make(chan *Part)
	f := func() {
		a.Lock()
		defer a.Unlock()
		defer close(pc)
		for _, v := range a.Parts {
			if v.ID == id {
				pc <- v
				return
			}
		}
	}
	go f()
	return pc
}

func TotalSize() int64 {
	a := <-All()
	var s int64
	for _, x := range a {
		s += x.Size
	}
	return s
}

var list = &Listing{}
