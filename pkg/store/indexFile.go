package store
//
//import (
//	"github.com/wuzhc/gmq/pkg/utils"
//	"hash/crc32"
//	"os"
//	"runtime"
//	"syscall"
//)
//
//const (
//	INDEX_HEADER_SIZE = 40
//	INDEX_SLOT_NUM    = 5000000
//	INDEX_SLOT_SIZE   = 4
//	INDEX_LINKED_SIZE = 20
//)
//
//
//type indexHeader struct {
//	beginTimestampIndex int64
//	endTimestampIndex   int64
//	beginPhyoffsetIndex int64
//	endPhyoffsetIndex   int64
//	hashSlotcountIndex  int32
//	indexCount          int32
//}
//
//type indexSlotTable struct {
//	indexCount int32
//}
//
//type indexLinkedList struct {
//	hashCode  int32
//	phyOffset int64
//	timeDiff  int32
//	prevIndex int32
//}
//
//// Index header + slot table + Index Linked List
//type IndexFile struct {
//	header *indexHeader
//	slot   *indexSlotTable
//	linked *indexLinkedList
//	file   *os.File
//	buffer mappedByteBuffer
//	count  int64
//}
//
//func NewIndexFile(path string) (*IndexFile, error) {
//	indexFile := &IndexFile{}
//	var fd *os.File
//	var err error
//	if exist, _ := utils.PathExists(path); !exist {
//		fd, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
//	} else {
//		fd, err = os.OpenFile(path, os.O_RDWR, 0600)
//	}
//	if err != nil {
//		return nil, err
//	}
//
//	stat, err := fd.Stat()
//	if err != nil {
//		return nil, err
//	}
//
//	initMmapSize := int(stat.Size())
//	if initMmapSize == 0 {
//		// new file need to extend size
//		if _, err := fd.WriteAt([]byte{'0'}, int64(os.Getpagesize())-1); err != nil {
//			return nil, err
//		}
//
//		if runtime.GOOS != "windows" {
//			if err := syscall.Fdatasync(int(indexFile.file.Fd())); err != nil {
//				return nil, err
//			}
//		} else {
//			if err := indexFile.file.Sync(); err != nil {
//				return nil, err
//			}
//		}
//
//		initMmapSize = INDEX_HEADER_SIZE + INDEX_SLOT_SIZE*INDEX_SLOT_NUM
//	}
//
//	// 解除上一次映射,如果有的话
//	if indexFile.buffer.Size() > 0 {
//		if err := unmapIndexFile(indexFile); err != nil {
//			return nil, err
//		}
//	}
//
//	if err := mmapIndexFile(indexFile, initMmapSize); err != nil {
//		return nil, err
//	}
//
//	return indexFile, nil
//}
//
//func (idx *IndexFile) SearchPhyOffset(key string) (int64, error) {
//	hashCode := generateHashCode(key)
//	slotPos := hashCode % INDEX_SLOT_NUM
//	absSlotPos := INDEX_HEADER_SIZE + slotPos*INDEX_SLOT_SIZE // find position for slot
//	index, err := idx.buffer.Uint32(absSlotPos)
//	if err != nil {
//		return -1, err
//	}
//
//	// find position for index
//	indexPos := INDEX_HEADER_SIZE + INDEX_SLOT_NUM*INDEX_SLOT_SIZE + int(index)*INDEX_LINKED_SIZE
//	keyHashRead, err := idx.buffer.Uint32(indexPos)
//	phyOffsetRead,err:=idx.buffer.Uint64(indexPos+4)
//	timeDiffRead,err:=idx.buffer.Uint32(indexPos+4+8)
//	prevIndex,err:=idx.buffer.Uint32(indexPos+4+8+4)
//
//	for prevIndex>0 {
//
//	}
//
//	return -1, nil
//}
//
//func generateHashCode(key string) int {
//	v := int(crc32.ChecksumIEEE([]byte(key)))
//	if v >= 0 {
//		return v
//	}
//	if -v >= 0 {
//		return -v
//	}
//	return 0
//}
