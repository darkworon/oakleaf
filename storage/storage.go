package storage

import (
	//"oakleaf/cluster"
	//	"io/ioutil"
	"oakleaf/config"
	"oakleaf/files"
	"oakleaf/utils"
	//	"os"
	"path/filepath"
	//	"sort"
)

var Files = &files.List{}

//type File *files.File
func PartsCount(c *config.Config) int {
	return utils.GetFilesCount(filepath.Join(c.WorkingDir, c.ConfigFile))

}
