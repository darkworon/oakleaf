package partstorage

import (
	"io/ioutil"
	"os"
	"sort"
	"oakleaf/config"
	"sync"
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

}

var list = &AwaitingList{}