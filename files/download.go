package files

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	//"oakleaf/cluster"
	"oakleaf/cluster/node/client"
	"oakleaf/utils"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/darkworon/oakleaf/storage"
)

func (f *File) Download(w *http.ResponseWriter, ratio int64) (err error) {
	//var buf = make([]byte, f.Size)
	if f.IsAvailable() {
		//	var readers []io.Reader
		log.Debugf("Downloading file \"%s\" - contains from %d parts", f.Name, len(f.Parts))
		partCounter := 0
		for _, v := range f.Parts {
			//fmt.Println(v)
			if &v != nil && v.GetMainNode() != nil {
				log.Debugf("Getting part #%d - %s from server %s...", partCounter, v.ID, v.GetMainNode().Address)
				node := v.GetMainNode()
				//temp_buf, err := v.GetData()
				if !node.IsActive {
					//fmt.Println("[WARN] MainNode is not available, trying to get data from replica nodes...")
					node = v.FindLiveNode()
					if node == nil {
						err = fmt.Errorf("No nodes available to download part %s, can't finish download", v.ID)
						utils.HandleError(err)
						return err
					}
				}
				partCounter++

				resp, err := client.Get(fmt.Sprintf("%s://%s/part/%s", node.Protocol(), node.Address, storage.GetURI(v.ID)))
				if err != nil {
					utils.HandleError(err)
					return err
					//	break
				}
				defer resp.Body.Close()
				if resp.StatusCode != 200 {
					err = fmt.Errorf("Node %s not have parts %s", node.Address, v.ID)
					return err
				}
				log.Debugf("Streaming parts #%d - %s to the client", partCounter, v.ID)
				//	readers = append(readers, resp.Body)
				for i := v.Size; i > 0; i -= ratio / 10 {
					if _, err = io.CopyN(*w, resp.Body, ratio/10); err != nil && err != io.EOF {
						utils.HandleError(err)
						return err
					}
					time.Sleep(100 * time.Millisecond)
				}
				//time.Sleep(2 * time.Second)
				partCounter++
			} else {
				err = fmt.Errorf("No nodes available to download parts %s, can't finish download", v)
				log.Error(err)
				return err
			}
		}
	} else {
		err = errors.New(fmt.Sprintf("[ERR] Not all nodes available to download files %s", f.ID))
		return err
	}
	return err
}
