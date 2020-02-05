package qdata

import (
	"bytes"
	"encoding/binary"
	"fundqin/common"
	"fundqin/log"
	"go.uber.org/zap"
	"os"
	"sync"
)

type Index struct {
	Date int32
	Pos  int32
	Len  int32
}

type IndexInfo struct {
	Indexs   []Index
	DataSize int32
}

var INDEX_MAP sync.Map

func (ii *IndexInfo) SearchIndex(date int32) *Index {
	return binaryFind(&ii.Indexs, 0, len(ii.Indexs)-1, date)
}

func InitIndex(security string, index *Index) (*IndexInfo, *Index) {

	index.Pos = 0

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, index.Date)
	binary.Write(buf, binary.LittleEndian, index.Pos)
	binary.Write(buf, binary.LittleEndian, index.Len)
	binary.Write(buf, binary.LittleEndian, common.INDEX_END)

	indexBytes := make([]byte, common.INDEX_OFFSET)
	copy(indexBytes, buf.Bytes())

	var ii = IndexInfo{}
	ii.Indexs = append(ii.Indexs, index)
	ii.DataSize = index.Len
	//put to cache
	INDEX_MAP.Store(security, &ii)

	//write to file
	writeFile(security, indexBytes)

	log.Logger.Info("InitIndex ", zap.String("security", security))

	return &ii, &index
}

func (ii *IndexInfo) AddIndex(insertIndex Index) *Index {
	var findi = -1
	for i, index := range ii.Indexs {
		if insertIndex.Date > index.Date {
			findi = i
			break
		}
	}

	inseri := findi + 1
	ii.Indexs = insertSlice(ii.Indexs, insertIndex, inseri)

	ii.reCalPos()

	return &ii.Indexs[inseri]
}

func insertSlice(slice []Index, insertion Index, insertPos int) []Index {
	result := make([]Index, len(slice)+1)
	at := copy(result, slice[:insertPos])
	at += copy(result[at:], []Index{insertion})
	copy(result[at:], slice[insertPos:])
	return result
}

func (ii *IndexInfo) reCalPos() *IndexInfo {
	for i, index := range ii.Indexs {
		if i == 0 {
			index.Pos = 0
		} else {
			lastIndex := ii.Indexs[i-1]
			index.Pos = lastIndex.Pos + lastIndex.Len
		}

	}
	return ii
}

func GetIndex(security string) (*IndexInfo, error) {
	if v, ok := INDEX_MAP.Load(security); ok {
		return (v).(*IndexInfo), nil
	}

	indexBytes, err := readFile(security)
	if err != nil {
		return nil, err
	}
	if indexBytes == nil {
		//index empty!
		return nil, nil
	}
	var ii = IndexInfo{}
	//decode bytes
	ii.deCode(indexBytes)

	//put to cache
	INDEX_MAP.Store(security, &ii)

	return &ii, nil
}

func readFile(security string) ([]byte, error) {
	file, err := os.OpenFile(common.DATA_PATH+security+".dat", os.O_RDWR, 0)
	if err != nil {
		//file not exist
		return nil, nil
	}

	defer file.Close()

	file.Seek(0, 0)

	bytes := make([]byte, common.INDEX_OFFSET)
	_, err = file.Read(bytes)
	if err != nil {
		return nil, err
	}

	return bytes, nil
}

func writeFile(security string, indexBytes []byte) error {
	file, _ := os.OpenFile(common.DATA_PATH+security+".dat", os.O_CREATE, 0)

	defer file.Close()

	file.Seek(0, 0)

	_, err := file.Write(indexBytes)
	if err != nil {
		return err
	}

	return nil
}
func (ii *IndexInfo) deCode(indexBytes []byte) *IndexInfo {
	bufData := bytes.NewBuffer(indexBytes)

	var readLen = int32(0)
	for {
		if bufData.Len() == 0 {
			break
		}
		var index = Index{}
		binary.Read(bufData, binary.LittleEndian, &index.Date)
		binary.Read(bufData, binary.LittleEndian, &index.Pos)
		binary.Read(bufData, binary.LittleEndian, &index.Len)

		readLen += index.Len
		ii.Indexs = append(ii.Indexs, index)

		end, _ := bufData.ReadByte()
		if end == common.INDEX_END {
			break
		} else {
			bufData.UnreadByte()
		}

	}
	ii.DataSize = readLen

	return ii
}

func binaryFind(arr *[]Index, leftIndex int, rightIndex int, date int32) *Index {
	if leftIndex > rightIndex {
		return nil
	}
	middle := (leftIndex + rightIndex) / 2

	if (*arr)[middle].Date > date {
		binaryFind(arr, leftIndex, middle-1, date)
	} else if (*arr)[middle].Date < date {
		binaryFind(arr, middle+1, rightIndex, date)
	} else {
		return &((*arr)[middle])
	}
	return nil
}
