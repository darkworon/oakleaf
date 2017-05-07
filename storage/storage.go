package storage

import (
	//"oakleaf/cluster"
	//	"io/ioutil"
	"oakleaf/config"
	"oakleaf/utils"
	//	"os"
	"path/filepath"
	//	"sort"
	"github.com/satori/go.uuid"
	"strings"
	"sync"
	"sort"
	"time"
	"encoding/json"
	"io/ioutil"
	"fmt"
)

type Parts []*Part

type Part struct {
	ID        string    `json:"id"`
	Path      string    `json:"path"`
	Size      int64     `json:"size"`
	CreatedAt time.Time `json:"created_at"`
}

type Listing struct {
	indexLock sync.RWMutex        `json:"-"`
	sync.RWMutex        `json:"-"`
	parts     Parts        `json:"parts"`
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
		l := (<-New()).parts
		l = append(l, list.parts...)
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
	list.parts = append(list.parts, p)
	list.Unlock()
	go Save()
}

func Delete(p *Part) {
	list.Lock()
	a := list.parts
	for i, x := range a {
		if x.ID == p.ID {
			a = append(a[:i], a[i+1:]...)
			list.parts = a
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
	return len(list.parts)
}

func Save() {
	data := All().Json()
	fmt.Println("Saving...")
	//list.indexLock.Lock()
	ioutil.WriteFile(filepath.Join(config.Get().WorkingDir, "parts.json"), data, 0644)
	//list.indexLock.Unlock()
	fmt.Println("Done!")

}

func Import(dir, name string) (int, error) {
	list.Lock()
	list.indexLock.Lock()
	defer list.Unlock()
	defer list.indexLock.Unlock()
	a := list.parts
	var _json, err = utils.LoadExistingFile(filepath.Join(dir, name))
	err = json.Unmarshal(_json, &a)
	if err != nil {
		utils.HandleError(err)
	}
	list.parts = a
	return Count(), err

}

var list = &Listing{}