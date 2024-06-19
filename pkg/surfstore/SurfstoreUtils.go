package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"sort"
)

type void struct{}
var member void

func Hash(buf []byte) string {
	h := sha256.New()
	h.Write(buf)
	return hex.EncodeToString(h.Sum(nil))
}

func getHashListAndBlockMap(client RPCClient, filename string) ([]string, map[string]*Block){
	blockMap := make(map[string]*Block, 0)
	hashlist := make([]string, 0)
	file, err := os.Open(ConcatPath(client.BaseDir, filename))
	if err != nil {
		// fmt.Println("Open File Error!", err)
		log.Panic(err)
	}
	defer file.Close()

	content, err := os.ReadFile(ConcatPath(client.BaseDir, filename))
	if err != nil {
		// fmt.Println("Read File Error!", err)
		log.Panic(err)
	}

	for i := 0; i < (len(content)+client.BlockSize-1)/client.BlockSize; i++ {
		var data []byte
		if (i+1)*client.BlockSize > len(content) {
			data = content[i*client.BlockSize : ]
		}else {
			data = content[i*client.BlockSize : (i+1)*client.BlockSize]
		}

		hash := Hash(data)
		hashlist = append(hashlist, hash)

		block := Block{}
		block.BlockData = data
		block.BlockSize = int32(len(data))
		blockMap[hash] = &block
	}

	return hashlist, blockMap
}

func CompareHashList(a, b []string) bool {
	return reflect.DeepEqual(a, b)
}

func getKeys(fileMap map[string]*FileMetaData) []string {
	keys := make([]string,0,len(fileMap))
	for k := range fileMap {
		keys = append(keys, k)
	}
	return keys
}

func in(target string, str_array []string) bool {
	sort.Strings(str_array)
	index := sort.SearchStrings(str_array, target)
	if index < len(str_array) && str_array[index] == target {
		return true
	}
	return false
}

func PathExist(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func download(client RPCClient, fileName string, bsAddr *string, cloudMap *map[string]*FileMetaData, fileMap *map[string]*FileMetaData) {
	hashlist := (*cloudMap)[fileName].GetBlockHashList()
	version := (*cloudMap)[fileName].GetVersion()
	delHashList := []string{"0"}

	if CompareHashList(hashlist, delHashList) {
		return
	}

	blocks := []byte{}
	for _, h := range hashlist {
		block := Block{}
		getBlockErr := client.GetBlock(h, *bsAddr, &block)
		if getBlockErr != nil {
			log.Panic(getBlockErr)
			return
		}
		blocks = append(blocks, block.BlockData...)
	}

	var f *os.File
	var err error
	path := ConcatPath(client.BaseDir, fileName)
	Exist, e := PathExist(path)
	if e != nil {
		log.Panic(e)
		return
	}
	if !Exist {
		f, err = os.Create(path)
	}else {
		f, err = os.Open(path)
	}

	if err != nil {
		log.Println("Open/Create File Error: ", err)
		return
	}
	defer f.Close()

	error := os.WriteFile(path, blocks, 0777)
	if error != nil {
		log.Println("Write File Error: ", error)
		return
	}

	newFileData := &FileMetaData{
		Filename: fileName,
		Version: version,
		BlockHashList: hashlist,
	}
	(*fileMap)[fileName] = newFileData

	writeErr := WriteMetaFile(*fileMap, client.BaseDir)
	if writeErr != nil {
		log.Panic(writeErr)
	}
}

func handleSets(client RPCClient, fileName string, bsAddr *string, cloudMap *map[string]*FileMetaData, t string, fileMap *map[string]*FileMetaData) {
	hashlist := []string{}
	blockmap := make(map[string]*Block)
	if t == "c" || t == "u" {
		hashlistOut := []string{}
		hashlist, blockmap = getHashListAndBlockMap(client, fileName)
		hasBlockErr := client.HasBlocks(hashlist, *bsAddr, &hashlistOut)
		if hasBlockErr != nil {
			log.Panic(hasBlockErr)
		}

		for _, h := range hashlist {
			if !in(h, hashlistOut) {
				block := blockmap[h]
				var succ bool
				putBlockErr := client.PutBlock(block, *bsAddr, &succ)
				if !succ || putBlockErr != nil {
					// fmt.Println("Put Block Error!")
					log.Panic(putBlockErr)
				}
			}
		}
	}

	var v int32
	delHashList := []string{"0"}
	var filedata FileMetaData
	switch t {
		case "c":
			filedata = FileMetaData{Filename: fileName, Version: int32(1), BlockHashList: hashlist}
		case "u":
			currData := (*cloudMap)[fileName]
			filedata = FileMetaData{Filename: fileName, Version: currData.Version+1, BlockHashList: hashlist}
		case "d":
			currData := (*cloudMap)[fileName]
			filedata = FileMetaData{Filename: fileName, Version: currData.Version+1, BlockHashList: delHashList}
	}

	updateErr := client.UpdateFile(&filedata, &v)
	if updateErr != nil {
		log.Panic(updateErr)
	}
	if v == int32(-1) {
		download(client, filedata.Filename, bsAddr, cloudMap, fileMap)
	}
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	path, _ :=  filepath.Abs(ConcatPath(client.BaseDir, DEFAULT_META_FILENAME))
	flag, err := PathExist(path)
	if err != nil {
		log.Panic(err)
		return
	}
	if !flag {
		_, err := os.Create(path)
		if err != nil {
			log.Panic(err)
			return
		}
	}
	// get fileInfoMap on Cloud
	cloudMap := make(map[string]*FileMetaData)
	getFileErr := client.GetFileInfoMap(&cloudMap)
	if getFileErr != nil {
		log.Panic(getFileErr)
	}
	// get blockstore addr on Cloud
	bsAddr := ""
	getBlockErr := client.GetBlockStoreAddr(&bsAddr)
	if getBlockErr != nil {
		log.Panic(getBlockErr)
	}

	// sets of handling different operations
	createSet := make(map[string]void)
	updateSet := make(map[string]void)
	deleteSet := make(map[string]void)
	// the sign of delete file
	delHashList := []string{"0"}
	// read the file in basedir
	filesInfo, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		// fmt.Println("Read BaseDirectory Error!", err)
		log.Panic(err)
		return
	}

	// local file meta map(index.txt)
	fileMap, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		// fmt.Printf("Load Local Metadata Error: %v", err)
		log.Panic(err)
		return
	}

	fileList := make([]string, len(filesInfo))
	for _, f := range filesInfo {
		fileName := f.Name()
		// skip the index.txt
		if fileName == "index.txt"{
			continue
		}
		// check file if in local meta map
		currData, ok := fileMap[fileName]
		if !ok {
			// NOT in the local meta map
			createSet[fileName] = member
		}else {
			// IN the local meta map
			// check the hashlist changed or not
			hashlist, _ := getHashListAndBlockMap(client, fileName)
			if !CompareHashList(hashlist, currData.GetBlockHashList()) {
				updateSet[fileName] = member
			}
		}
		fileList = append(fileList, fileName)
	}

	for _, f := range getKeys(fileMap) {
		// check file if in BaseDir
		flag := in(f, fileList)
		hlist := fileMap[f].BlockHashList
		if !flag {
			if !(len(hlist)==1 && hlist[0]=="0") {
				deleteSet[f] = member
			}
		}
	}

	// filter the set
	for f, d := range cloudMap {
		flag := in(f, getKeys(fileMap))
		if !flag {
			// NOT in the local meta map
			if createSet[f] == member {
				delete(createSet, f)	
			}
			download(client, f, &bsAddr, &cloudMap, &fileMap)
		}else {
			// IN the local meta map
			if d.Version != fileMap[f].Version {
				//deleteSet
				if deleteSet[f] == member{
					delete(deleteSet, f)
				}
				//updateSet
				if updateSet[f] == member{
					delete(updateSet, f)
				}
				if CompareHashList(d.BlockHashList, delHashList) {
					p := ConcatPath(client.BaseDir, f)
					Exist, e := PathExist(p)
					if e != nil {
						log.Panic(e)
						return
					}
					if Exist {
						delerr := os.Remove(p)
						if delerr != nil {
							log.Panic(delerr)
							return
						}
					}

					newFileData := &FileMetaData{
						Filename: f,
						Version: d.Version+1,
						BlockHashList: delHashList,
					}
					fileMap[f] = newFileData
				
					writeErr := WriteMetaFile(fileMap, client.BaseDir)
					if writeErr != nil {
						log.Panic(writeErr)
					}
				}else {
					download(client, f, &bsAddr, &cloudMap, &fileMap)
				}	
			}
		}
	}

	fmt.Printf("createSet: %v\n", createSet)
	fmt.Printf("updateSet: %v\n", updateSet)
	fmt.Printf("deleteSet: %v\n", deleteSet)

	// handle the set of created items
	for k := range createSet {
		handleSets(client, k, &bsAddr, &cloudMap, "c", &fileMap)
	}

	// handle the set of update items
	for k := range updateSet {
		handleSets(client, k, &bsAddr, &cloudMap, "u", &fileMap)
	}

	// handle the set of delete items
	for k := range deleteSet {
		handleSets(client, k, &bsAddr, &cloudMap, "d", &fileMap)
	}

	// helperfunc load new index.txt into local basedir
	getFileErr = client.GetFileInfoMap(&cloudMap)
	if getFileErr != nil {
		log.Panic(getFileErr)
	}

	writeErr := WriteMetaFile(cloudMap, client.BaseDir)
	if writeErr != nil {
		log.Panic(writeErr)
	}

}