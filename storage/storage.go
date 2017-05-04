package storage

import (
	//"oakleaf/cluster"
	"oakleaf/config"
	"oakleaf/file"
	"oakleaf/utils"
	"path/filepath"
)

var Config = &config.Config{}
var Files = &file.List{}

//type File *file.File
func PartsCount(c *config.Config) int {
	return utils.GetFilesCount(filepath.Join(c.WorkingDir, c.ConfigFile))

}
