package file

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	//"oakleaf/cluster"
	"oakleaf/utils"
	"time"
)

func (f *File) Download(w *http.ResponseWriter, ratio int64) (err error) {
	//var buf = make([]byte, f.Size)
	if f.IsAvailable() {
		//	var readers []io.Reader
		fmt.Printf("Downloading file \"%s\" - contains from %d parts\n", f.Name, len(f.Parts))
		partCounter := 0
		for _, v := range f.Parts {
			//fmt.Println(v)
			if &v != nil && v.GetMainNode() != nil {
				fmt.Printf("[MSINFO] Getting part #%d - %s from server %s...\n", partCounter, v.ID, v.GetMainNode().Address)
				node := v.GetMainNode()
				//temp_buf, err := v.GetData()
				if !node.IsActive {
					//fmt.Println("[WARN] MainNode is not available, trying to get data from replica nodes...")
					node = v.FindLiveReplica()
					if node == nil {
						err = errors.New(fmt.Sprintf("[ERR] No nodes available to download part %s, can't finish download.", v.ID))
						utils.HandleError(err)
						return err
					}
				}
				partCounter++
				/*
					tr := &http.Transport{
						MaxIdleConns:       10,
						IdleConnTimeout:    30 * time.Second,
						DisableCompression: true,
					}
					client := &http.Client{Transport: tr}
					resp, err := client.Get("https://example.com")*/
				resp, err := http.Get(fmt.Sprintf("http://%s/part/%s", node.Address, v.ID))
				if err != nil {
					utils.HandleError(err)
					return err
					//	break
				}
				defer resp.Body.Close()
				if resp.StatusCode != 200 {
					err = errors.New(fmt.Sprintf("Node %s not have part %s", node.Address, v.ID))
					return err
				}
				fmt.Printf("[MSINFO] Streaming part #%d - %s to the client \n", partCounter, v.ID)
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
				err = errors.New(fmt.Sprintf("[ERR] No nodes available to download part %s, can't finish download.", v))
				utils.HandleError(err)
				return err
			}

		}
		/*multiR := io.MultiReader(readers...)
		if err != nil {
			HandleError(err)
			return err
		}*/

	} else {
		err = errors.New(fmt.Sprintf("[ERR] Not all nodes available to download file %s", f.ID))
		return err
	}
	return err
}
