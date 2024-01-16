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
package datanode

import (
	"context"
	"errors"
	"fmt"
	dnapi "github.com/apache/ozone-go/api/proto/datanode"
	"github.com/apache/ozone-go/api/proto/hdds"
	"github.com/apache/ozone-go/api/proto/ratis"
	"google.golang.org/grpc"
	"math/rand"
	"strconv"
	"time"
)

type ClientTargetType int

const (
	STANDALONE_ROUND_ROBIN ClientTargetType = iota
	STANDALONE_RANDOM
	STANDALONE_FIX
	RATIS_LEADER
)

var ASYNC_QUEUE_SIZE = 5

type ChunkInfo struct {
	Name   string
	Offset uint64
	Len    uint64
}

type DatanodeClient struct {
	ratisClient        *ratis.RaftClientProtocolService_OrderedClient
	ratisReceiver      chan ratis.RaftClientReplyProto
	standaloneClient   *dnapi.XceiverClientProtocolService_SendClient
	standaloneReceiver chan dnapi.ContainerCommandResponseProto

	ratisRequests           chan *ratis.RaftClientRequestProto
	standaloneRequests      chan *dnapi.ContainerCommandRequestProto
	countRatisRequest       int
	countRatisResponse      int // For ratis.SlidingWindowEntry async response
	countStandaloneRequest  int
	countStandaloneResponse int
	responseNotify          chan int
	asynResponse            error

	ctx             context.Context
	datanodes       []*hdds.DatanodeDetailsProto
	currentDatanode hdds.DatanodeDetailsProto
	grpcConnection  *grpc.ClientConn
	pipelineId      *hdds.PipelineID
	memberIndex     int
	leaderId        *string
	targetType      ClientTargetType
	raftGroupId     []byte
	callId          uint64
	seqNum          uint64 // For ratis.SlidingWindowEntry
	isFirst         bool
	secTimeout      time.Duration
}

func (dn *DatanodeClient) GetCurrentDnUUid() *string {
	uid := dn.currentDatanode.GetUuid()
	return &uid
}

// TODO: to support ec bucket, to connect all datanodes of the pipeline
func (dnClient *DatanodeClient) connectToPipeline() error {
	return nil
}

// TODO: to get the latest leader of the pipeline from om
func (dnClient *DatanodeClient) connectToNextTarget() error {
	if dnClient.grpcConnection != nil {
		dnClient.Close()
	}
	dnClient.memberIndex += 1
	dnClient.currentDatanode = *dnClient.datanodes[dnClient.memberIndex]
	if dnClient.memberIndex == len(dnClient.datanodes) {
		dnClient.memberIndex = 0
	}

	clientPort := 0
	portName := ""
	if dnClient.targetType == RATIS_LEADER {
		portName = "RATIS"
		dnClient.ratisRequests = make(chan *ratis.RaftClientRequestProto, ASYNC_QUEUE_SIZE) // TODO: make it configurable, the pointer maybe error used, the request should be get from sync.Pool???
	} else {
		portName = "STANDALONE"
		//dnClient.standaloneRequests = make(chan *dnapi.ContainerCommandRequestProto, ASYNC_QUEUE_SIZE) // TODO: make it configurable, the pointer maybe error used, the request should be get from sync.Pool???
	}
	dnClient.responseNotify = make(chan int, 100)

	for _, port := range dnClient.currentDatanode.Ports {
		if *port.Name == portName {
			clientPort = int(*port.Value)
		}
	}

	address := *dnClient.currentDatanode.IpAddress + ":" + strconv.Itoa(clientPort)
	println("Connecting to the " + address)
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(128*1024*1024)))
	//conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(128*1024*1024))) // No-Block connection?
	if err != nil {
		return err
	}
	dnClient.grpcConnection = conn

	dnClient.ratisReceiver = make(chan ratis.RaftClientReplyProto)
	dnClient.standaloneReceiver = make(chan dnapi.ContainerCommandResponseProto)

	if dnClient.targetType == RATIS_LEADER {
		client, _ := ratis.NewRaftClientProtocolServiceClient(conn).Ordered(dnClient.ctx)
		dnClient.ratisClient = &client
		go dnClient.RaftReceive()
	} else {
		client, _ := dnapi.NewXceiverClientProtocolServiceClient(conn).Send(dnClient.ctx)
		dnClient.standaloneClient = &client
		go dnClient.StandaloneReceive()
	}

	return nil
}

func (dnClient *DatanodeClient) connectToTarget() error {
	if dnClient.grpcConnection != nil {
		dnClient.Close()
	}

	if dnClient.targetType == RATIS_LEADER {
		dnClient.memberIndex = 0
		for _, datanode := range dnClient.datanodes {
			// select the leader datanode
			if datanode.GetUuid() == *dnClient.leaderId {
				dnClient.currentDatanode = *datanode
				dnClient.memberIndex += 1
				break
			}
		}
	} else if dnClient.targetType == STANDALONE_RANDOM {
		dnClient.memberIndex = rand.Intn(len(dnClient.datanodes))
		dnClient.currentDatanode = *dnClient.datanodes[dnClient.memberIndex]
	} else if dnClient.targetType == STANDALONE_ROUND_ROBIN {
		dnClient.memberIndex += 1
		dnClient.currentDatanode = *dnClient.datanodes[dnClient.memberIndex]
		if dnClient.memberIndex == len(dnClient.datanodes) {
			dnClient.memberIndex = 0
		}
	}

	clientPort := 0
	portName := ""
	if dnClient.targetType == RATIS_LEADER {
		portName = "RATIS"
		dnClient.ratisRequests = make(chan *ratis.RaftClientRequestProto, ASYNC_QUEUE_SIZE) // TODO: make it configurable, the pointer maybe error used, the request should be get from sync.Pool???
	} else {
		portName = "STANDALONE"
		//dnClient.standaloneRequests = make(chan *dnapi.ContainerCommandRequestProto, ASYNC_QUEUE_SIZE) // TODO: make it configurable, the pointer maybe error used, the request should be get from sync.Pool???
	}
	dnClient.responseNotify = make(chan int, 100)

	for _, port := range dnClient.currentDatanode.Ports {
		if *port.Name == portName {
			clientPort = int(*port.Value)
		}
	}

	address := *dnClient.currentDatanode.IpAddress + ":" + strconv.Itoa(clientPort)
	println("Connecting to the " + address)
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock(),
		grpc.WithInitialWindowSize(5242880),
		grpc.WithInitialConnWindowSize(5177345),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(128*1024*1024)))
	//conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(128*1024*1024))) // No-Block connection?
	if err != nil {
		return err
	}
	dnClient.grpcConnection = conn

	dnClient.ratisReceiver = make(chan ratis.RaftClientReplyProto)
	dnClient.standaloneReceiver = make(chan dnapi.ContainerCommandResponseProto)

	if dnClient.targetType == RATIS_LEADER {
		client, _ := ratis.NewRaftClientProtocolServiceClient(conn).Ordered(dnClient.ctx)
		dnClient.ratisClient = &client
		go dnClient.RaftReceive()
	} else {
		client, _ := dnapi.NewXceiverClientProtocolServiceClient(conn).Send(dnClient.ctx)
		dnClient.standaloneClient = &client
		go dnClient.StandaloneReceive()
	}

	return nil
}

func (dnClient *DatanodeClient) connectToTargetEc(memberIndex int) error {
	if dnClient.grpcConnection != nil {
		dnClient.Close()
	}

	dnClient.memberIndex = memberIndex
	dnClient.currentDatanode = *dnClient.datanodes[dnClient.memberIndex]

	var clientPort int
	portName := "STANDALONE"

	//dnClient.responseNotify = make(chan int, 100)
	//dnClient.standaloneRequests = make(chan *dnapi.ContainerCommandRequestProto, ASYNC_QUEUE_SIZE)

	for _, port := range dnClient.currentDatanode.Ports {
		if *port.Name == portName {
			clientPort = int(*port.Value)
		}
	}

	address := *dnClient.currentDatanode.IpAddress + ":" + strconv.Itoa(clientPort)
	println("Connecting to the " + address)
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock(),
		grpc.WithInitialWindowSize(5242880),
		grpc.WithInitialConnWindowSize(5177345),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(128*1024*1024)))
	if err != nil {
		return err
	}
	dnClient.grpcConnection = conn

	dnClient.standaloneReceiver = make(chan dnapi.ContainerCommandResponseProto)

	client, _ := dnapi.NewXceiverClientProtocolServiceClient(conn).Send(dnClient.ctx)
	dnClient.standaloneClient = &client
	go dnClient.StandaloneReceive()

	return nil
}

func CreateDatanodeClient(pipeline *hdds.Pipeline, targetType ClientTargetType) (*DatanodeClient, error) {
	dnClient := &DatanodeClient{
		ctx:         context.Background(),
		pipelineId:  pipeline.Id,
		datanodes:   pipeline.Members,
		memberIndex: -1,
		leaderId:    pipeline.LeaderID,
		isFirst:     true,
		targetType:  targetType,
		secTimeout:  10, // 10 seconds, to be configurable
	}
	err := dnClient.connectToTarget()
	if err != nil {
		return nil, err
	}
	return dnClient, nil
}

func CreateDatanodeClientEc(pipeline *hdds.Pipeline, memberIndex int) (*DatanodeClient, error) {
	dnClient := &DatanodeClient{
		ctx:         context.Background(),
		pipelineId:  pipeline.Id,
		datanodes:   pipeline.Members,
		memberIndex: -1,
		leaderId:    pipeline.LeaderID,
		isFirst:     true,
		targetType:  STANDALONE_FIX,
		secTimeout:  10, // 10 seconds, to be configurable
	}
	err := dnClient.connectToTargetEc(memberIndex)
	if err != nil {
		return nil, err
	}
	return dnClient, nil
}

func (dnClient *DatanodeClient) CreateAndWriteChunk(id *dnapi.DatanodeBlockID, blockOffset uint64, buffer []byte, length uint64, chunkId int) (dnapi.ChunkInfo, error) {
	bpc := uint32(12)
	checksumType := dnapi.ChecksumType_NONE
	checksumDataProto := dnapi.ChecksumData{
		Type:             &checksumType,
		BytesPerChecksum: &bpc,
	}
	chunkName := fmt.Sprintf("%d_chunk_%d", *id.LocalID, chunkId)
	chunkInfoProto := dnapi.ChunkInfo{
		ChunkName:    &chunkName,
		Offset:       &blockOffset,
		Len:          &length,
		ChecksumData: &checksumDataProto,
	}
	return dnClient.WriteChunk(id, chunkInfoProto, buffer, length)
}

func (dnClient *DatanodeClient) WriteChunk(id *dnapi.DatanodeBlockID, info dnapi.ChunkInfo, data []byte, length uint64) (dnapi.ChunkInfo, error) {

	req := dnapi.WriteChunkRequestProto{
		BlockID:   id,
		ChunkData: &info,
		Data:      data[:length],
	}
	commandType := dnapi.Type_WriteChunk
	uuid := dnClient.currentDatanode.GetUuid()
	proto := dnapi.ContainerCommandRequestProto{
		CmdType:      &commandType,
		WriteChunk:   &req,
		ContainerID:  id.ContainerID,
		DatanodeUuid: &uuid,
	}

	_, err := dnClient.sendDatanodeCommand(proto)
	if err != nil {
		return info, err
	}
	return info, nil
}

func (dnClient *DatanodeClient) ReadChunk(id *dnapi.DatanodeBlockID, info ChunkInfo) ([]byte, error) {
	result := make([]byte, 0)

	bpc := uint32(12)
	checksumType := dnapi.ChecksumType_NONE
	checksumDataProto := dnapi.ChecksumData{
		Type:             &checksumType,
		BytesPerChecksum: &bpc,
	}
	chunkInfoProto := dnapi.ChunkInfo{
		ChunkName:    &info.Name,
		Offset:       &info.Offset,
		Len:          &info.Len,
		ChecksumData: &checksumDataProto,
	}
	req := dnapi.ReadChunkRequestProto{
		BlockID:   id,
		ChunkData: &chunkInfoProto,
	}
	commandType := dnapi.Type_ReadChunk
	uuid := dnClient.currentDatanode.GetUuid()
	proto := dnapi.ContainerCommandRequestProto{
		CmdType:      &commandType,
		ReadChunk:    &req,
		ContainerID:  id.ContainerID,
		DatanodeUuid: &uuid,
	}

	resp, err := dnClient.sendDatanodeCommand(proto)
	if err != nil {
		return result, err
	}
	if resp.GetResult() != dnapi.Result_SUCCESS {
		return nil, errors.New(resp.GetResult().String() + " " + resp.GetMessage())
	}
	return resp.GetReadChunk().GetData(), nil
}

func (dnClient *DatanodeClient) PutBlock(id *dnapi.DatanodeBlockID, chunks []*dnapi.ChunkInfo, eof bool) error {

	flags := int64(0)
	req := dnapi.PutBlockRequestProto{
		BlockData: &dnapi.BlockData{
			BlockID:  id,
			Flags:    &flags,
			Metadata: make([]*dnapi.KeyValue, 0),
			Chunks:   chunks,
		},
		Eof: &eof,
	}
	commandType := dnapi.Type_PutBlock
	proto := dnapi.ContainerCommandRequestProto{
		CmdType:      &commandType,
		PutBlock:     &req,
		ContainerID:  id.ContainerID,
		DatanodeUuid: dnClient.GetCurrentDnUUid(),
	}

	_, err := dnClient.sendDatanodeCommand(proto)
	if err != nil {
		return err
	}
	return nil
}

func (dnClient *DatanodeClient) GetBlock(id *dnapi.DatanodeBlockID) ([]ChunkInfo, error) {
	result := make([]ChunkInfo, 0)

	req := dnapi.GetBlockRequestProto{
		BlockID: id,
	}
	commandType := dnapi.Type_GetBlock
	proto := dnapi.ContainerCommandRequestProto{
		CmdType:      &commandType,
		GetBlock:     &req,
		ContainerID:  id.ContainerID,
		DatanodeUuid: dnClient.GetCurrentDnUUid(),
	}

	resp, err := dnClient.sendDatanodeCommand(proto)
	if err != nil {
		return result, err
	}
	if resp.GetResult() != dnapi.Result_SUCCESS {
		return nil, errors.New(resp.GetResult().String() + " " + resp.GetMessage())
	}
	for _, chunkInfo := range resp.GetGetBlock().GetBlockData().Chunks {
		result = append(result, ChunkInfo{
			Name:   chunkInfo.GetChunkName(),
			Offset: chunkInfo.GetOffset(),
			Len:    chunkInfo.GetLen(),
		})
	}
	return result, nil
}

func (dnClient *DatanodeClient) sendDatanodeCommand(proto dnapi.ContainerCommandRequestProto) (dnapi.ContainerCommandResponseProto, error) {
	if dnClient.targetType == RATIS_LEADER {
		return dnClient.sendRatisDatanodeCommand(proto)
	} else {
		return dnClient.sendStandaloneDatanodeCommand(proto)
	}
}

func (dnClient *DatanodeClient) CheckRecvDatanodeCommandResponse() {
	if dnClient.targetType == RATIS_LEADER {
		for {
			req := <-dnClient.ratisRequests
			if req == nil { // dnClient closed
				break
			}
			timeout := time.After(time.Second * dnClient.secTimeout)
			select {
			case resp := <-dnClient.ratisReceiver:
				//fmt.Println("Ratis response: ", resp.RpcReply.Success)
				dnClient.countRatisResponse += 1
				//fmt.Println("Ratis response count: ", dnClient.countRatisResponse, "request length:", len(dnClient.ratisRequests))
				dnClient.responseNotify <- dnClient.countRatisResponse
				if !resp.RpcReply.Success {
					dnClient.asynResponse = errors.New("Ratis command failed")
					break
				}
			case <-timeout: // For a request, if timeout, we just return error
				dnClient.asynResponse = errors.New("Ratis command timeout")
				break
			}
		}
	} else {
		for true {
			req := <-dnClient.standaloneRequests
			if req == nil { // dnClient closed
				break
			}
			timeout := time.After(time.Second * dnClient.secTimeout)
			select {
			case resp := <-dnClient.standaloneReceiver:
				dnClient.countStandaloneResponse += 1
				dnClient.responseNotify <- dnClient.countStandaloneResponse
				if resp.GetResult() != dnapi.Result_SUCCESS {
					dnClient.asynResponse = errors.New("standalone command failed")
				}
			case <-timeout:
				dnClient.asynResponse = errors.New("standalone command timeout")
			}
		}
	}
}

func (dnClient *DatanodeClient) CheckDatanodeCommandCompletion() {
	if dnClient.targetType == RATIS_LEADER {
		for true {
			timeout := time.After(time.Second * dnClient.secTimeout)
			select {
			case <-dnClient.responseNotify:
				if dnClient.countRatisResponse == dnClient.countRatisRequest {
					return
				}
				//fmt.Println("CheckDatanodeCommandCompletion: ", dnClient.countRatisResponse, "/", dnClient.countRatisRequest)
			case <-timeout:
				//fmt.Println("CheckDatanodeCommandCompletion timeout")
				dnClient.asynResponse = errors.New("Ratis command timeout")
			}
		}
	} else {
		for true {
			timeout := time.After(time.Second * dnClient.secTimeout)
			select {
			case <-dnClient.responseNotify:
				if dnClient.countStandaloneResponse == dnClient.countStandaloneRequest {
					return
				}
			case <-timeout:
				dnClient.asynResponse = errors.New("Ratis command timeout")
			}
		}
	}
}

func (dnClient *DatanodeClient) CheckDatanodeCommandWindowReady() {
	if dnClient.targetType == RATIS_LEADER {
		for true {
			timeout := time.After(time.Second * dnClient.secTimeout)
			select {
			case <-dnClient.responseNotify:
				if dnClient.countRatisResponse == dnClient.countRatisRequest {
					return
				}
			case <-timeout:
				dnClient.asynResponse = errors.New("Ratis command timeout")
			}
		}
	} else {
		for true {
			timeout := time.After(time.Second * dnClient.secTimeout)
			select {
			case <-dnClient.responseNotify:
				if dnClient.countRatisResponse == dnClient.countRatisRequest {
					return
				}
			case <-timeout:
				dnClient.asynResponse = errors.New("Ratis command timeout")
			}
		}
	}
}

func (dn *DatanodeClient) Close() {
	if dn.targetType == RATIS_LEADER {
		(*dn.ratisClient).CloseSend()

	} else {
		(*dn.standaloneClient).CloseSend()
	}
	if dn.ratisRequests != nil {
		close(dn.ratisRequests)
		dn.ratisRequests = nil
	}
	if dn.standaloneRequests != nil {
		close(dn.standaloneRequests)
		dn.standaloneRequests = nil
	}
	close(dn.responseNotify)
	dn.grpcConnection.Close()
	dn.grpcConnection = nil
}
