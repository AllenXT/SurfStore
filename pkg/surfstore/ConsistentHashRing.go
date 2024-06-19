package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strconv"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) InsertServer(addr string) {
	server_hash := c.Hash(addr)
	c.ServerMap[server_hash] = addr[len(addr)-1:]
}

func (c ConsistentHashRing) DeleteServer(addr string) {
	server_hash := c.Hash(addr)
	delete(c.ServerMap, server_hash)
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	// Find the next largest key from ServerMap
	var sortedkeys []string
	resServer := ""
	for k := range c.ServerMap {
		sortedkeys = append(sortedkeys, k)
	}
	sort.Strings(sortedkeys)
	for i := 0; i < len(sortedkeys); i++ {
		if blockId < sortedkeys[i] {
			resServer = c.ServerMap[sortedkeys[i]]
			break
		}
	}
	if resServer == "" {
		resServer = c.ServerMap[sortedkeys[0]]
	}
	return resServer
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func (c ConsistentHashRing) OutputMap(blockHashes []string) map[string]string {
	res := make(map[string]string)
	for i := 0; i < len(blockHashes); i++ {
		res["block"+strconv.Itoa(i)] = c.GetResponsibleServer(blockHashes[i])
	}
	return res
}

func NewConsistentHashRing(numServers int, downServer []int) *ConsistentHashRing {
	c := &ConsistentHashRing{
		ServerMap: make(map[string]string),
	}

	for i := 0; i < numServers; i++ {
		c.InsertServer("blockstore" + strconv.Itoa(i))
	}

	for i := 0; i < len(downServer); i++ {
		c.DeleteServer("blockstore" + strconv.Itoa(downServer[i]))
	}

	return c
}
