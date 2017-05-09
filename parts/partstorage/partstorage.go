package partstorage

import (
	"fmt"
	"io/ioutil"
	"oakleaf/config"
	"oakleaf/storage"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"strings"

	log "github.com/Sirupsen/logrus"
)

type filePart os.FileInfo
type fileParts []os.FileInfo

type AwaitingList struct {
	sync.RWMutex
	items []*string
}

func Parts() fileParts {
	p, _ := ioutil.ReadDir(config.Get().DataDir)
	return p
}

func (p fileParts) DescSort() fileParts {
	sort.Slice(p, func(i, j int) bool {
		return (p[i]).Size() > (p[j]).Size()
	})
	return p
}

func (p fileParts) AscSort() fileParts {
	sort.Slice(p, func(i, j int) bool {
		return (p[i]).Size() < (p[j]).Size()
	})
	return p
}

func PartsCount() int {
	return len(Parts())
}

func All() *AwaitingList {
	return list
}

func Add(s string) {
	var me = All()
	me.Lock()
	defer me.Unlock()
	me.items = append(me.items, &s)
}

func IsIn(s string) bool {
	var me = All()
	me.Lock()
	defer me.Unlock()
	for _, x := range me.items {
		if *x == s {
			return true
		}
	}
	return false
}

func Del(s string) {
	var me = All()
	me.Lock()
	defer me.Unlock()
	for i, x := range me.items {
		if *x == s {
			copy(me.items[i:], me.items[i+1:])
			me.items[len(me.items)-1] = nil // or the zero value of T
			me.items = me.items[:len(me.items)-1]
		}
	}
	//fmt.Println(me)

}

//
//Load - reads (recursively) data storage folder, gets files from it and adds to storage list.
//
func Load() {
	searchDir := config.Get().DataDir
	fileList := []string{}
	err := filepath.Walk(searchDir, func(path string, f os.FileInfo, err error) error {
		if !f.IsDir() && len(f.Name()) == 28 {
			storage.Add(&storage.Part{
				ID:        PathToID(path),
				Path:      storage.GetFullPath(PathToID(path)),
				Size:      f.Size(),
				CreatedAt: time.Now(),
			})
		}
		return nil
	})
	if err != nil {
		log.Error(err)
	}
	for _, file := range fileList {
		fmt.Println(file)
	}
	fmt.Println(fileList)
}

//PathToID converts part path to ID.
// i.e. .../node1/data/ad/12/6c0ab9df45f7a52c7179185e3a01 will be converted
// into ad126c0a-b9df-45f7-a52c-7179185e3a01
func PathToID(path string) string {

	str_ar := strings.Split(path, "/")
	str := strings.Join(str_ar[len(str_ar)-3:], "")
	a := str[:8]
	b := str[8:12]
	c := str[12:16]
	d := str[16:20]
	e := str[20:]
	full_uuid := fmt.Sprintf("%s-%s-%s-%s-%s", a, b, c, d, e)
	return full_uuid
}

var list = &AwaitingList{}
