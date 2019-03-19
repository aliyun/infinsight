package connector

import (
	"bufio"
	"fmt"
	"os"

	"inspector/client"
	"inspector/util"
	"inspector/util/unsafe"
)

type fileConnector struct {
	directory string // file address
	addr      string // filepath
	offset    int64  // seek position

	client client.ClientInterface // file client
}

func (fc *fileConnector) Get() (interface{}, error) {
	f := fc.client.GetSession().(*os.File)
	if _, err := f.Seek(fc.offset, 0); err != nil {
		return nil, err
	}

	var output [][]byte
	reader := bufio.NewReader(f)
	for {
		data, err := reader.ReadBytes(byte(util.Newline))
		if err != nil {
			return nil, err
		}

		fc.offset += int64(len(data) + 1)
		if unsafe.Bytes2String(data) == util.DebugFileSpilter {
			break
		}
		output = append(output, data)
	}
	return output, nil
}

func (fc *fileConnector) Close() {
	fc.client.Close()
}

func (fc *fileConnector) ensureNetwork() error {
	if fc.client != nil {
		return nil
	}

	var err error
	filename := fmt.Sprintf("%s/%s", fc.directory, fc.addr)
	if fc.client, err = client.NewClient(util.File, filename, "", ""); err != nil {
		return fmt.Errorf("create client with db-type[%s] address[%s] error[%v]", util.File, filename, err)
	}

	return nil
}
