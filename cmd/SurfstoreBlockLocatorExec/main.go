package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"cse224/proj4/pkg/surfstore"
	"crypto/sha256"
	"encoding/hex"
)

func Hash(buf []byte) string {
	h := sha256.New()
	h.Write(buf)
	return hex.EncodeToString(h.Sum(nil))
}

func getHashList(filename string, BlockSize int) []string{
	hashlist := make([]string, 0)
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("Open File Error!", err)
		log.Panic(err)
	}
	defer file.Close()

	content, err := os.ReadFile(filename)
	if err != nil {
		fmt.Println("Read File Error!", err)
		log.Panic(err)
	}

	for i := 0; i < (len(content)+BlockSize-1)/BlockSize; i++ {
		var data []byte
		if (i+1)*BlockSize > len(content) {
			data = content[i*BlockSize : ]
		}else {
			data = content[i*BlockSize : (i+1)*BlockSize]
		}

		hash := Hash(data)
		hashlist = append(hashlist, hash)
	}

	return hashlist
}

func main() {
	var downServersList []int
	var out []string
	output := "{"

	downServers := flag.String("downServers", "", "Comma-separated list of server IDs that have failed")
	flag.Parse()

	if flag.NArg() != 3 {
		fmt.Printf("Usage: %s numServers blockSize inpFilename\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	numServers, err := strconv.Atoi(flag.Arg(0))
	if err != nil {
		log.Fatal("Invalid number of servers argument: ", flag.Arg(0))
	}

	blockSize, err := strconv.Atoi(flag.Arg(1))
	if err != nil {
		log.Fatal("Invalid block size argument: ", flag.Arg(0))
	}

	inpFilename := flag.Arg(2)

	log.Println("Total number of blockStore servers: ", numServers)
	log.Println("Block size: ", blockSize)
	log.Println("Processing input data filename: ", inpFilename)

	if *downServers != "" {
		for _, downServer := range strings.Split(*downServers, ",") {
			log.Println("Server ", downServer, " is in a failed state")
			downServerInt, err := strconv.Atoi(downServer)
			if err != nil {
				log.Panic(err)
			}
			downServersList = append(downServersList, downServerInt)
		}
	} else {
		log.Println("No servers are in a failed state")
	}

	// This is an example of the format of the output
	// Your program will emit pairs for each block has where the
	// first part of the pair is the block hash, and the second
	// element is the server number that the block resides on
	//
	// This output is simply to show the format, the actual mapping
	// isn't based on consistent hashing necessarily
	// fmt.Println("{{672e9bff6a0bc59669954be7b2c2726a74163455ca18664cc350030bc7eca71e, 7}, {31f28d5a995dcdb7c5358fcfa8b9c93f2b8e421fb4a268ca5dc01ca4619dfe5f,2}, {172baa036a7e9f8321cb23a1144787ba1a0727b40cb6283dbb5cba20b84efe50,1}, {745378a914d7bcdc26d3229f98fc2c6887e7d882f42d8491530dfaf4effef827,5}, {912b9d7afecb114fdaefecfa24572d052dde4e1ad2360920ebfe55ebf2e1818e,0}}")

	// fmt.Printf("numServers: %v\n", numServers)
	// fmt.Printf("downServersList: %v\n", downServersList)
	ConsistentHashRing := surfstore.NewConsistentHashRing(numServers, downServersList)
	hlist := getHashList(inpFilename, blockSize)

	blockMap := make(map[string]string)
	for i := 0; i < len(hlist); i++ {
		blockMap["block"+strconv.Itoa(i)] = hlist[i]
	}

	outputMap := ConsistentHashRing.OutputMap(hlist)
	for i := 0; i < len(hlist); i++ {
		str := "{" + blockMap["block"+strconv.Itoa(i)] + "," + outputMap["block"+strconv.Itoa(i)] + "}"
		out = append(out, str)
	}
	for i,s := range out {
		if i == len(out)-1 {
			output = output + s
		}else {
			output = output + s + ","
		}
	}
	output += "}"

	fmt.Printf("%v\n", output)
}
