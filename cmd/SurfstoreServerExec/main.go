package main

import (
	"cse224/proj4/pkg/surfstore"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"google.golang.org/grpc"
)

// Usage String
const USAGE_STRING = "./run-server.sh -s <service_type> -p <port> -l -d (blockStoreAddr*)"

// Set of valid services
var SERVICE_TYPES = map[string]bool{"meta": true, "block": true, "both": true}

// Exit codes
const EX_USAGE int = 64

func main() {
	// Custom flag Usage message
	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage of %s:\n", USAGE_STRING)
		flag.VisitAll(func(f *flag.Flag) {
			fmt.Fprintf(w, "  -%s: %v\n", f.Name, f.Usage)
		})
		fmt.Fprintf(w, "  (blockStoreAddr*): BlockStore Address (include self if service type is both)\n")
	}

	// Parse command-line argument flags
	service := flag.String("s", "", "(required) Service Type of the Server: meta, block, both")
	port := flag.Int("p", 8080, "(default = 8080) Port to accept connections")
	localOnly := flag.Bool("l", false, "Only listen on localhost")
	debug := flag.Bool("d", false, "Output log statements")
	flag.Parse()

	// Use tail arguments to hold BlockStore address
	args := flag.Args()
	blockStoreAddr := ""
	if len(args) == 1 {
		blockStoreAddr = args[0]
	}

	// Valid service type argument
	if _, ok := SERVICE_TYPES[strings.ToLower(*service)]; !ok {
		flag.Usage()
		os.Exit(EX_USAGE)
	}

	// Add localhost if necessary
	addr := ""
	if *localOnly {
		addr += "localhost"
	}
	addr += ":" + strconv.Itoa(*port)

	// Disable log outputs if debug flag is missing
	if !(*debug) {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}

	log.Fatal(startServer(addr, strings.ToLower(*service), blockStoreAddr))
}

func startServer(hostAddr string, serviceType string, blockStoreAddr string) error {
	//Create a new Server
	grpcServer := grpc.NewServer()

	//Register rpc services
	if serviceType == "both" {
		surfstore.RegisterBlockStoreServer(grpcServer, surfstore.NewBlockStore())
		surfstore.RegisterMetaStoreServer(grpcServer, surfstore.NewMetaStore(blockStoreAddr))
	}else if serviceType == "meta" {
		surfstore.RegisterMetaStoreServer(grpcServer, surfstore.NewMetaStore(blockStoreAddr))
	}else {
		surfstore.RegisterBlockStoreServer(grpcServer, surfstore.NewBlockStore())
	}
	
	//start listening on hostAddr
	l, err := net.Listen("tcp", hostAddr)
	if err != nil {
		return err
	}
	return grpcServer.Serve(l)
}
