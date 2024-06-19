package surfstore

import (
	context "context"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	info_map := FileInfoMap{}
	InfoMap := make(map[string]*FileMetaData,0)

	for k, v := range m.FileMetaMap {
		InfoMap[k] = &FileMetaData{
			Filename: v.Filename, 
			Version: v.Version,
			BlockHashList: v.BlockHashList,
		}
	}
	
	info_map.FileInfoMap = InfoMap
	return &info_map, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	v := Version{}
	newData := &FileMetaData{
		Filename: fileMetaData.Filename,
		Version: fileMetaData.Version,
		BlockHashList: fileMetaData.BlockHashList,
	}
	oldData, ok := m.FileMetaMap[fileMetaData.Filename]
	if !ok || fileMetaData.Version == oldData.Version + 1 {
		m.FileMetaMap[fileMetaData.Filename] = newData
		v.Version = newData.Version
	}else {
		v.Version = int32(-1)
	}
	return &v, nil
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	bsAddr := BlockStoreAddr{}
	bsAddr.Addr = m.BlockStoreAddr
	return &bsAddr, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
