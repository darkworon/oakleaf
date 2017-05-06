package partstorage

import (
	"io/ioutil"
	"os"
	"sort"
	"oakleaf/config"
)

type filePart os.FileInfo
type fileParts []os.FileInfo

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
