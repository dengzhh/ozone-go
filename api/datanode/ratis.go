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
	"encoding/binary"
	"fmt"
	"github.com/apache/ozone-go/api/proto/datanode"
	dnapi "github.com/apache/ozone-go/api/proto/datanode"
	"github.com/apache/ozone-go/api/proto/ratis"
	protobuf "github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"io"
	"time"
)

func (dnClient *DatanodeClient) sendRatisDatanodeCommand(proto datanode.ContainerCommandRequestProto) (datanode.ContainerCommandResponseProto, error) {
	_uuid, _ := uuid.Parse(dnClient.pipelineId.GetId())
	id, _ := _uuid.MarshalBinary()
	group := ratis.RaftGroupIdProto{
		Id: id,
	}
	requestId, _ := uuid.New().MarshalBinary()
	replyId := []byte(*dnClient.leaderId)

	watchRequestType := ratis.WatchRequestTypeProto{
		Index:       0,
		Replication: ratis.ReplicationLevel_MAJORITY,
	}
	watchType := ratis.RaftClientRequestProto_Watch{
		Watch: &watchRequestType,
	}

	var raft ratis.RaftClientRequestProto
	if dnClient.isFirst {
		request := ratis.RaftRpcRequestProto{
			RequestorId: requestId,
			ReplyId:     replyId,
			RaftGroupId: &group,
			CallId:      0,
			ToLeader:    true,
			SlidingWindowEntry: &ratis.SlidingWindowEntry{
				SeqNum:  1,
				IsFirst: true,
			},
		}
		raft = ratis.RaftClientRequestProto{
			RpcRequest: &request,
			Type:       &watchType,
		}
		dnClient.isFirst = false
		dnClient.callId = 1
		dnClient.seqNum = 2
		resp, err := dnClient.sendRatisMessage(raft)
		if err != nil {
			return datanode.ContainerCommandResponseProto{}, err
		}
		if resp.RpcReply != nil && resp.RpcReply.Success != true {
			// Only for non-aysnc write
			return datanode.ContainerCommandResponseProto{}, fmt.Errorf("failed to send first message")
		}
	}

	var data []byte
	if proto.GetCmdType() == dnapi.Type_WriteChunk {
		data = proto.WriteChunk.Data
		proto.WriteChunk.Data = nil
	} else if proto.GetCmdType() == dnapi.Type_PutSmallFile {
		data = proto.PutSmallFile.Data
		proto.PutSmallFile.Data = nil
	}

	bytes, err := protobuf.Marshal(&proto)

	if err != nil {
		return datanode.ContainerCommandResponseProto{}, err
	}

	lengthHeader := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthHeader, uint32(len(bytes)))
	if proto.GetCmdType() == dnapi.Type_WriteChunk || proto.GetCmdType() == dnapi.Type_PutSmallFile {
		bytes = append(bytes, data...)
	}

	message := ratis.ClientMessageEntryProto{
		Content: append(lengthHeader, bytes...),
	}

	writeRequestType := ratis.WriteRequestTypeProto{}
	writeType := ratis.RaftClientRequestProto_Write{
		Write: &writeRequestType,
	}

	request := ratis.RaftRpcRequestProto{
		RequestorId: requestId,
		ReplyId:     replyId,
		RaftGroupId: &group,
		CallId:      dnClient.callId,
		ToLeader:    true,
		SlidingWindowEntry: &ratis.SlidingWindowEntry{
			SeqNum:  dnClient.seqNum,
			IsFirst: false,
		},
	}

	raft = ratis.RaftClientRequestProto{
		RpcRequest: &request,
		Message:    &message,
		Type:       &writeType,
	}

	resp, err := dnClient.sendRatisMessageAsyn(raft)
	if err != nil {
		return datanode.ContainerCommandResponseProto{}, err
	}
	dnClient.callId += 1
	dnClient.seqNum += 1
	containerResponse := datanode.ContainerCommandResponseProto{}
	/* TODO: to remvoe unused code for aysnc operation */
	if resp.Message != nil { // Only for sync operation
		err = protobuf.Unmarshal(resp.Message.Content, &containerResponse)
	}
	if err != nil {
		return containerResponse, err
	}
	return containerResponse, nil
}

func (dnClient *DatanodeClient) sendRatisMessage(request ratis.RaftClientRequestProto) (ratis.RaftClientReplyProto, error) {
	err := (*dnClient.ratisClient).Send(&request)
	if err != nil {
		return ratis.RaftClientReplyProto{}, err
	}

	resp := ratis.RaftClientReplyProto{}
	timeout := time.After(time.Second * dnClient.secTimeout)
	select {
	case resp = <-dnClient.ratisReceiver:
	case <-timeout:
		err = fmt.Errorf("sendRatisMessage timeout")
	}

	if resp.GetNotLeaderException() != nil {
		// TODO: to get the latest leader and connect to it
		err = dnClient.connectToNextTarget()
		if err != nil {
			return ratis.RaftClientReplyProto{}, err
		}
		err = (*dnClient.ratisClient).Send(&request)
		if err != nil {
			return ratis.RaftClientReplyProto{}, err
		}
		select {
		case resp = <-dnClient.ratisReceiver:
		case <-timeout:
			err = fmt.Errorf("sendRatisMessageToServer timeout")
		}
	}
	if err != nil {
		return ratis.RaftClientReplyProto{}, err
	}

	return resp, nil
}

func (dnClient *DatanodeClient) sendRatisMessageAsyn(request ratis.RaftClientRequestProto) (ratis.RaftClientReplyProto, error) {
	err := (*dnClient.ratisClient).Send(&request)
	if err != nil {
		return ratis.RaftClientReplyProto{}, err
	}

	if dnClient.ratisRequests != nil {
		dnClient.ratisRequests <- &request
		dnClient.countRatisRequest += 1
		//fmt.Println("sendRatisMessageAsyn", len(request.Message.Content), dnClient.countRatisRequest)
		return ratis.RaftClientReplyProto{}, nil
	}
	return ratis.RaftClientReplyProto{}, fmt.Errorf("ratisRequests is nil, aysnc is not supported")
}

func (dnClient *DatanodeClient) RaftReceive() {
	for {
		proto, err := (*dnClient.ratisClient).Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			// rpc error: code = Unavailable desc = transport is closing
			// rpc error: code = Canceled desc = grpc: the client connection is closing
			//fmt.Println(err)
			return
		}
		dnClient.ratisReceiver <- *proto
	}
}
