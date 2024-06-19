# SurfStore

## Overview

SurfStore is a cloud-based file storage service inspired by Dropbox, enabling users to sync files to and from the "cloud". It comprises a cloud service and a client that interacts with the service via gRPC. Multiple clients can concurrently connect to SurfStore to access and update a shared set of files, ensuring consistency across clients.

## System Components

### BlockStore Service

The BlockStore service handles the storage and retrieval of file content blocks. It provides the following API:

* `PutBlock(b)`: Stores block b indexed by its hash value.
* `GetBlock(h)`: Retrieves the block indexed by hash value h.
* `HasBlocks(hashlist_in)`: Returns a subset of hashlist_in that are stored in the key-value store.

### MetaStore Service

The MetaStore service manages file metadata, including the mapping of filenames to hashlists and version numbers. It provides the following API:

* `GetFileInfoMap()`: Returns a mapping of files stored in the cloud.
* `UpdateFile()`: Updates the file metadata if the version number is correct.
* `GetBlockStoreAddr()`: Returns the address of the BlockStore.

## Key Concepts

### Blocks and HashLists

Files are divided into blocks of uniform size, with each block being hashed using SHA-256. The ordered list of these hashes, known as a hashlist, represents the file. This allows for efficient storage and retrieval of file content.

### Versioning

Each file has a version number that increments with every modification. This versioning system helps clients detect and resolve conflicts by ensuring they are always working with the most up-to-date file version.

### Conflict Resolution

When multiple clients modify a file concurrently, SurfStore uses version numbers to ensure that the first client to sync its changes to the cloud wins. Subsequent clients must resolve conflicts by updating their local copies with the latest changes from the cloud.

## Consistent Hashing for Scalability

To handle large-scale deployments, SurfStore uses consistent hashing to map blocks to BlockStores. This ensures efficient data distribution and retrieval without bottlenecks. The system can handle failed servers by remapping blocks to available servers, maintaining data accessibility.

## Project Structure

* `/cmd` - Contains the main executables for the client and server.
* `/pkg` - Contains the implementation of BlockStore and MetaStore services.
* `/...` - Other necessary files and directories for the project.

## Additional Information

SurfStore aims to provide a robust and scalable cloud storage solution, leveraging concepts such as consistent hashing and versioning to ensure data integrity and availability. The project demonstrates the implementation of a cloud-based file storage service that can handle multiple clients and large-scale data efficiently.
