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
package om

import (
	"errors"
	"github.com/apache/ozone-go/api/common"
	"github.com/apache/ozone-go/api/proto/hdds"
	ozone_proto "github.com/apache/ozone-go/api/proto/ozone"
)

func (om *OmClient) CreateBucket(volume string, bucket string) error {
	isVersionEnabled := false
	storageType := ozone_proto.StorageTypeProto_DISK
	bucketInfo := ozone_proto.BucketInfo{
		BucketName:       &bucket,
		VolumeName:       &volume,
		IsVersionEnabled: &isVersionEnabled,
		StorageType:      &storageType,
	}
	req := ozone_proto.CreateBucketRequest{
		BucketInfo: &bucketInfo,
	}

	cmdType := ozone_proto.Type_CreateBucket
	wrapperRequest := ozone_proto.OMRequest{
		CmdType:             &cmdType,
		CreateBucketRequest: &req,
		ClientId:            &om.clientId,
	}

	_, err := om.submitRequest(&wrapperRequest)
	if err != nil {
		return err
	}
	return nil
}

func (om *OmClient) GetBucket(volume string, bucket string) (common.BucketInfo, error) {
	req := ozone_proto.InfoBucketRequest{
		VolumeName: &volume,
		BucketName: &bucket,
	}

	cmdType := ozone_proto.Type_InfoBucket
	wrapperRequest := ozone_proto.OMRequest{
		CmdType:           &cmdType,
		InfoBucketRequest: &req,
		ClientId:          &om.clientId,
	}

	resp, err := om.submitRequest(&wrapperRequest)
	if err != nil {
		return common.BucketInfo{}, err
	}
	b := common.BucketInfo{
		Name:       *resp.InfoBucketResponse.BucketInfo.BucketName,
		VolumeName: *resp.InfoBucketResponse.BucketInfo.VolumeName,
	}
	if resp.InfoBucketResponse.BucketInfo.DefaultReplicationConfig != nil {
		b.ReplicationType = *resp.InfoBucketResponse.BucketInfo.DefaultReplicationConfig.Type
		if b.ReplicationType == hdds.ReplicationType_RATIS {
			if resp.InfoBucketResponse.BucketInfo.DefaultReplicationConfig.Factor != nil {
				b.Replication = *resp.InfoBucketResponse.BucketInfo.DefaultReplicationConfig.Factor
			} else {
				b.Replication = hdds.ReplicationFactor_THREE
			}
		} else if b.ReplicationType == hdds.ReplicationType_STAND_ALONE {
			b.Replication = hdds.ReplicationFactor_ONE
		} else if b.ReplicationType == hdds.ReplicationType_EC {
			if resp.InfoBucketResponse.BucketInfo.DefaultReplicationConfig.EcReplicationConfig != nil {
				b.EcData = *resp.InfoBucketResponse.BucketInfo.DefaultReplicationConfig.EcReplicationConfig.Data
				b.EcParity = *resp.InfoBucketResponse.BucketInfo.DefaultReplicationConfig.EcReplicationConfig.Parity
				b.EcChunkSize = *resp.InfoBucketResponse.BucketInfo.DefaultReplicationConfig.EcReplicationConfig.EcChunkSize
				b.EcCodec = *resp.InfoBucketResponse.BucketInfo.DefaultReplicationConfig.EcReplicationConfig.Codec
			} else {
				b.EcData = 3
				b.EcParity = 2
				b.EcChunkSize = 1048576
				b.EcCodec = "RS"
			}
		} else {
			return common.BucketInfo{}, errors.New("unsupported replication type")
		}
	} else {
		b.ReplicationType = hdds.ReplicationType_RATIS
		b.Replication = hdds.ReplicationFactor_THREE
	}

	return b, nil
}

func (om *OmClient) ListBucket(volume string) ([]common.BucketInfo, error) {
	res := make([]common.BucketInfo, 0)

	req := ozone_proto.ListBucketsRequest{
		VolumeName: &volume,
		StartKey:   ptr(""),
		Count:      ptri(100),
	}

	cmdType := ozone_proto.Type_ListBuckets
	wrapperRequest := ozone_proto.OMRequest{
		CmdType:            &cmdType,
		ListBucketsRequest: &req,
		ClientId:           &om.clientId,
	}

	resp, err := om.submitRequest(&wrapperRequest)
	if err != nil {
		return res, err
	}
	for _, b := range resp.ListBucketsResponse.BucketInfo {
		cb := common.BucketInfo{
			Name:       *b.BucketName,
			VolumeName: *b.VolumeName,
		}
		res = append(res, cb)
	}
	return res, nil
}
