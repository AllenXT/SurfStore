package surfstore

import (
	context "context"
	"fmt"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	b, ok := bs.BlockMap[blockHash.Hash]
	block := Block{}

	if !ok {
		return nil, fmt.Errorf("Hash %v is not found", blockHash.Hash)
	}else {
		block.BlockData = b.BlockData
		block.BlockSize = b.BlockSize
		return &block, nil
	}
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	h := GetBlockHashString(block.BlockData)
	bs.BlockMap[h] = block
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	bh := BlockHashes{}
	hashes := make([]string, 0)
	for _, h := range blockHashesIn.GetHashes() {
		_, ok := bs.BlockMap[h]
		if !ok {
			continue
		}else {
			hashes = append(hashes, h)
		}
	}
	bh.Hashes = hashes
	return &bh, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
