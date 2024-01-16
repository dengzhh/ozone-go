// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package api

import (
	"errors"
	"fmt"
	"github.com/apache/ozone-go/api/common"
	"github.com/apache/ozone-go/api/datanode"
	dnproto "github.com/apache/ozone-go/api/proto/datanode"
	"github.com/apache/ozone-go/api/proto/hdds"
	omproto "github.com/apache/ozone-go/api/proto/ozone"
	"github.com/klauspost/reedsolomon"
	"io"
	"io/fs"
	"os"
	"strings"
	"sync"
	"time"
)

// From Hadoop ISA-L coder
const MMAX int = 14
const KMAX int = 10

var chunkPool = sync.Pool{
	New: func() interface{} {
		//buf := make([]byte, 32<<10)
		buf := make([]byte, 4<<20)
		return &buf
	},
}
var shardsPool = sync.Pool{
	New: func() interface{} {
		buf := make([][]byte, MMAX+MMAX) // For XOR coder: MMAX+KMAX, For RS coder: MMAX+MMAX
		return &buf
	},
}
var chunkPools = Cache{
	pools: make(map[int]*sync.Pool),
}

type Cache struct {
	pools map[int]*sync.Pool
}

func NewCache() *Cache {
	return &Cache{
		pools: make(map[int]*sync.Pool),
	}
}

func (c *Cache) Get(size int) []byte {
	pool, ok := c.pools[size]
	if !ok {
		pool = &sync.Pool{
			New: func() interface{} {
				return make([]byte, size)
			},
		}
		c.pools[size] = pool
	}
	return pool.Get().([]byte)
}

func (c *Cache) Put(size int, item []byte) {
	pool, ok := c.pools[size]
	if !ok {
		return
	}
	pool.Put(item)
}

func (ozoneClient *OzoneClient) ListKeys(volume string, bucket string) ([]common.KeyInfo, error) {

	keys, err := ozoneClient.OmClient.ListKeys(volume, bucket)
	if err != nil {
		return make([]common.KeyInfo, 0), err
	}

	ret := make([]common.KeyInfo, 0)
	for _, r := range keys {
		ret = append(ret, KeyFromProto(r))
	}
	return ret, nil

}

func (ozoneClient *OzoneClient) ListKeysPrefix(volume string, bucket string, prefix string) ([]common.KeyInfo, error) {
	keys, err := ozoneClient.OmClient.ListKeysPrefix(volume, bucket, prefix)
	if err != nil {
		return make([]common.KeyInfo, 0), err
	}

	ret := make([]common.KeyInfo, 0)
	for _, r := range keys {
		ret = append(ret, KeyFromProto(r))
	}
	return ret, nil
}

func (ozoneClient *OzoneClient) ListStatus(volume string, bucket string, prefix string) ([]O3fsFileInfo, error) {
	keys, err := ozoneClient.OmClient.ListStatus(volume, bucket, prefix, "", 1000, false)
	if err != nil {
		return make([]O3fsFileInfo, 0), err
	}

	ret := make([]O3fsFileInfo, 0)
	for _, r := range keys {
		ret = append(ret, KeyFromFileProto(r))
	}
	return ret, nil
}

func (ozoneClient *OzoneClient) InfoKey(volume string, bucket string, key string) (common.KeyInfo, error) {
	k, err := ozoneClient.OmClient.GetKey(volume, bucket, key)
	if err != nil {
		return common.KeyInfo{}, err
	}
	return KeyFromProto(k), err
}

func (ozoneClient *OzoneClient) InfoFile(volume string, bucket string, key string) (common.KeyInfo, error) {
	k, err := ozoneClient.OmClient.LookupFile(volume, bucket, key)
	if err != nil {
		return common.KeyInfo{}, err
	}
	return KeyFromProto(k), err
}

func (ozoneClient *OzoneClient) GetKey(volume string, bucket string, key string, destination io.Writer) (common.KeyInfo, error) {
	keyInfo, err := ozoneClient.OmClient.GetKey(volume, bucket, key)

	if err != nil {
		return common.KeyInfo{}, err
	}

	if len(keyInfo.KeyLocationList) == 0 {
		return common.KeyInfo{}, errors.New("Get key returned with zero key location version " + volume + "/" + bucket + "/" + key)
	}

	if len(keyInfo.KeyLocationList[0].KeyLocations) == 0 {
		return common.KeyInfo{}, errors.New("Key location doesn't have any datanode for key " + volume + "/" + bucket + "/" + key)
	}
	for _, location := range keyInfo.KeyLocationList[0].KeyLocations {
		pipeline := location.Pipeline

		dnBlockId := ConvertBlockId(location.BlockID)
		dnClient, err := datanode.CreateDatanodeClient(pipeline, datanode.STANDALONE_RANDOM)
		chunks, err := dnClient.GetBlock(dnBlockId)
		if err != nil {
			return common.KeyInfo{}, err
		}
		for _, chunk := range chunks {
			data, err := dnClient.ReadChunk(dnBlockId, chunk)
			if err != nil {
				return common.KeyInfo{}, err
			}
			destination.Write(data)
		}
		dnClient.Close()
	}
	return common.KeyInfo{}, nil
}

func ConvertBlockId(bid *hdds.BlockID) *dnproto.DatanodeBlockID {
	id := dnproto.DatanodeBlockID{
		ContainerID: bid.ContainerBlockID.ContainerID,
		LocalID:     bid.ContainerBlockID.LocalID,
	}
	return &id
}

func putKeyRatis(ozoneClient *OzoneClient, createKey *omproto.CreateKeyResponse, source io.Reader) (common.KeyInfo, error) {
	buffer := chunkPool.Get().(*[]byte)
	defer chunkPool.Put(buffer)

	locations := make([]*omproto.KeyLocation, 0)
	keyInfo := createKey.KeyInfo
	totalKeySize := uint64(0)
	location := keyInfo.KeyLocationList[0].KeyLocations[0] // For key size not determined put key, we only have one location
	for true {
		blockKeySize := uint64(0)
		pipeline := location.Pipeline
		blockId := ConvertBlockId(location.BlockID)
		blockOffset := *location.Offset
		chunkId := 1
		chunks := make([]*dnproto.ChunkInfo, 0)
		eof := false
		dnClient, err := datanode.CreateDatanodeClient(pipeline, datanode.RATIS_LEADER)
		go dnClient.CheckRecvDatanodeCommandResponse()
		if err != nil {
			return common.KeyInfo{}, err
		}
		for offset := blockOffset; offset < *location.Length+*location.Offset; chunkId += 1 {
			count, err := source.Read(*buffer)
			if err == io.EOF {
				eof = true
			} else if err != nil {
				return common.KeyInfo{}, err
			}
			if count > 0 {
				chunk, err := dnClient.CreateAndWriteChunk(blockId, offset, (*buffer)[:count], uint64(count), chunkId)
				if err != nil {
					return common.KeyInfo{}, err
				}
				offset += uint64(count)
				blockKeySize += uint64(count)
				chunks = append(chunks, &chunk)
			}
			if eof {
				break
			}
		}

		err = dnClient.PutBlock(blockId, chunks, eof)
		if err != nil {
			return common.KeyInfo{}, err
		}

		locations = append(locations, &omproto.KeyLocation{
			BlockID:  location.BlockID,
			Pipeline: location.Pipeline,
			Length:   &blockKeySize,
			Offset:   &blockOffset,
		})
		totalKeySize += blockKeySize

		dnClient.CheckDatanodeCommandCompletion()

		// TODO: comment the following line will cause the connection leak?
		dnClient.Close()

		if eof {
			break
		}
		nextBlockResponse, err := ozoneClient.OmClient.AllocateBlock(*keyInfo.VolumeName, *keyInfo.BucketName, *keyInfo.KeyName, createKey.ID, *keyInfo.Type, *keyInfo.Factor)
		if err != nil {
			return common.KeyInfo{}, err
		}
		location = nextBlockResponse.KeyLocation
	}

	ozoneClient.OmClient.CommitKey(*keyInfo.VolumeName, *keyInfo.BucketName, *keyInfo.KeyName, createKey.ID, locations, totalKeySize)
	return common.KeyInfo{}, nil
}

type EcCodec struct {
	Name      string
	Data      int
	Parity    int
	ChunkSize int
	Codec     interface{}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func AllocAligned(shards, each int) [][]byte {
	eachAligned := ((each + 63) / 64) * 64
	total := make([]byte, eachAligned*shards+63)
	// We cannot do initial align without "unsafe", just use native alignment.
	res := make([][]byte, shards)
	for i := range res {
		res[i] = total[:each:eachAligned]
		total = total[eachAligned:]
	}
	return res
}

func putKeyEc(ozoneClient *OzoneClient, createKey *omproto.CreateKeyResponse, source io.Reader) (common.KeyInfo, error) {
	ecCodec := EcCodec{
		Name:      strings.ToUpper(*createKey.KeyInfo.EcReplicationConfig.Codec),
		Data:      int(*createKey.KeyInfo.EcReplicationConfig.Data),
		Parity:    int(*createKey.KeyInfo.EcReplicationConfig.Parity),
		ChunkSize: int(*createKey.KeyInfo.EcReplicationConfig.EcChunkSize),
	}
	bufferShards := chunkPools.Get(ecCodec.ChunkSize * (ecCodec.Data + ecCodec.Parity))
	defer chunkPools.Put(ecCodec.ChunkSize*(ecCodec.Data+ecCodec.Parity), bufferShards)
	shards := *shardsPool.Get().(*[][]byte)
	datashardsSize := ecCodec.ChunkSize * ecCodec.Data
	defer shardsPool.Put(shards)

	var err error
	locations := make([]*omproto.KeyLocation, 0)
	keyInfo := createKey.KeyInfo
	totalKeySize := uint64(0)
	location := keyInfo.KeyLocationList[0].KeyLocations[0] // For key size not determined, we have only one location, the next location get by AllocateBlock()
	for {
		blockKeySize := uint64(0)
		pipeline := location.Pipeline
		blockId := ConvertBlockId(location.BlockID)
		chunkId := 1
		eofChunk := 0
		dnClients := make([]*datanode.DatanodeClient, len(pipeline.Members))
		shardOffsets := make([]uint64, len(pipeline.Members))
		chunks := make([][]*dnproto.ChunkInfo, len(pipeline.Members))
		for i := 0; i < len(pipeline.Members); i++ {
			shardOffsets[i] = *location.Offset
		}

		for offset := *location.Offset; offset < *location.Length+*location.Offset; chunkId += 1 {
			actualShardSize := 0
			count := 0
			var wg sync.WaitGroup
			results := make(chan error, len(pipeline.Members))

			b := bufferShards[:]
			count, err = source.Read(b[:datashardsSize])
			if err == io.EOF || count < datashardsSize { // TODO: preRead to check if eof
				eofChunk = chunkId
			} else if err != nil {
				return common.KeyInfo{}, err
			}
			blockKeySize += uint64(count)
			if count == 0 {
				break // TODO: to putBlock(eof=true)?
			}
			if count < ecCodec.ChunkSize {
				actualShardSize = count
			} else {
				actualShardSize = ecCodec.ChunkSize
			}
			actualDatashardsSize := actualShardSize * ecCodec.Data
			if count < actualDatashardsSize {
				for i := count; i < actualDatashardsSize; i++ {
					b[i] = 0
				}
			}
			if ecCodec.Codec == nil {
				ecCodec.Codec, err = reedsolomon.New(ecCodec.Data, ecCodec.Parity,
					reedsolomon.WithAutoGoroutines(actualShardSize), reedsolomon.WithCauchyMatrix())
			}
			for i := 0; i < ecCodec.Data+ecCodec.Parity; i++ {
				shards[i] = b[:actualShardSize]
				b = b[actualShardSize:]
				if i >= ecCodec.Data {
					shardOffsets[i] += uint64(actualShardSize)
				} else if count > actualShardSize {
					shardOffsets[i] += uint64(actualShardSize)
					count -= actualShardSize
				} else if count > 0 {
					shardOffsets[i] += uint64(count)
					count = 0
				}
			}
			if strings.ToUpper(*createKey.KeyInfo.EcReplicationConfig.Codec) == "RS" {
				err = ecCodec.Codec.(reedsolomon.Encoder).Encode(shards[:(ecCodec.Data + ecCodec.Parity)])
				if err != nil {
					return common.KeyInfo{}, err
				}
			} else {
				return common.KeyInfo{}, fmt.Errorf("Codec not supported:", *createKey.KeyInfo.EcReplicationConfig.Codec)
			}

			for i := 0; i < len(pipeline.Members); i++ {
				if dnClients[i] == nil {
					dnClients[i], err = datanode.CreateDatanodeClientEc(pipeline, i)
					if err != nil {
						return common.KeyInfo{}, err
					}
				}
				if shardOffsets[i] > offset {
					wg.Add(1)
					go func(shardIndex int, size uint64) {
						chunk, err := dnClients[shardIndex].CreateAndWriteChunk(blockId, offset, shards[shardIndex][:size], size, chunkId)
						if err == nil {
							chunks[shardIndex] = append(chunks[shardIndex], &chunk)
							err = dnClients[shardIndex].PutBlock(blockId, chunks[shardIndex], eofChunk > 0)
							fmt.Println("shard:", shardIndex, *chunk.Len)
						}
						results <- err
						wg.Done()
					}(i, shardOffsets[i]-offset)
				}
			}
			wg.Wait()
			close(results)
			for result := range results {
				if result != nil {
					return common.KeyInfo{}, result
				}
			}

			offset += uint64(actualShardSize)
			if eofChunk > 0 {
				break
			}
		}

		locations = append(locations, &omproto.KeyLocation{
			BlockID:  location.BlockID,
			Pipeline: location.Pipeline,
			Length:   &blockKeySize,
			Offset:   location.Offset,
		})
		totalKeySize += blockKeySize

		if eofChunk > 0 {
			break
		}
		nextBlockResponse, err := ozoneClient.OmClient.AllocateBlock(*keyInfo.VolumeName, *keyInfo.BucketName, *keyInfo.KeyName, createKey.ID, *keyInfo.Type, *keyInfo.Factor)
		if err != nil {
			return common.KeyInfo{}, err
		}
		location = nextBlockResponse.KeyLocation
	}

	ozoneClient.OmClient.CommitKey(*keyInfo.VolumeName, *keyInfo.BucketName, *keyInfo.KeyName, createKey.ID, locations, totalKeySize)
	return common.KeyInfo{}, nil
}

// TODO: to cache dnClient
func (ozoneClient *OzoneClient) PutKey(volume string, bucket string, key string, source io.Reader) (common.KeyInfo, error) {
	// TODO: to cache the bucket info
	bucketInfo, err := ozoneClient.OmClient.GetBucket(volume, bucket)
	if err != nil {
		return common.KeyInfo{}, err
	}
	createKey, err := ozoneClient.OmClient.CreateKey(volume, bucket, key, &bucketInfo)
	if err != nil {
		return common.KeyInfo{}, err
	}

	if bucketInfo.ReplicationType == hdds.ReplicationType_RATIS {
		return putKeyRatis(ozoneClient, createKey, source)
	} else if bucketInfo.ReplicationType == hdds.ReplicationType_EC {
		return putKeyEc(ozoneClient, createKey, source)
	} else {
		return common.KeyInfo{}, fmt.Errorf("Not supported")
	}
}

func (ozoneClient *OzoneClient) DeleteKey(volume string, bucket string, key string) error {
	_, err := ozoneClient.OmClient.DeleteKey(volume, bucket, key)
	return err
}

func KeyFromProto(keyProto *omproto.KeyInfo) common.KeyInfo {
	result := common.KeyInfo{
		Name:        *keyProto.KeyName,
		Replication: *keyProto.Type,
		VolumeName:  *keyProto.VolumeName,
		BucketName:  *keyProto.BucketName,
		Size:        *keyProto.DataSize,
		CTime:       *keyProto.CreationTime,
		MTime:       *keyProto.ModificationTime,
		IsDir:       false,
	}
	return result
}

type O3fsFileInfo struct {
	name  string
	size  int64
	mode  fs.FileMode
	mtime time.Time
	isDir bool
}

func (fi *O3fsFileInfo) Name() string {
	return fi.name
}

func (fi *O3fsFileInfo) Size() int64 {
	return fi.size
}

func (fi *O3fsFileInfo) Mode() fs.FileMode {
	return fi.mode
}

func (fi *O3fsFileInfo) ModTime() time.Time {
	return fi.mtime
}

func (fi *O3fsFileInfo) IsDir() bool {
	return fi.isDir
}

func (fi *O3fsFileInfo) Sys() interface{} {
	return nil
}

func (fi *O3fsFileInfo) Owner() string {
	return "hadoop"
}

func (fi *O3fsFileInfo) OwnerGroup() string {
	return "hadoop"
}

func KeyFromFileProto(keyProto *omproto.OzoneFileStatusProto) O3fsFileInfo {
	result := O3fsFileInfo{
		name:  *keyProto.KeyInfo.KeyName,
		size:  int64(*keyProto.KeyInfo.DataSize),
		mtime: time.UnixMilli(int64(*keyProto.KeyInfo.CreationTime)),
		isDir: *keyProto.IsDirectory,
		mode:  os.ModePerm,
	}
	return result
}
