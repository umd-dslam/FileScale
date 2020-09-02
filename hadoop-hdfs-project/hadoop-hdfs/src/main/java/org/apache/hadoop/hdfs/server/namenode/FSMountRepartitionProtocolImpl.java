/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.VersionedProtocol;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import org.apache.hadoop.hdfs.DFSUtil;

import org.apache.hadoop.hdfs.server.namenode.FsImageProto.MountPartition;

import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

import org.apache.commons.lang3.tuple.ImmutablePair;
import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;

public class FSMountRepartitionProtocolImpl implements FSMountRepartitionProtocol {
    @Override
    public void recordMove(byte[] data) throws IOException {
        MountPartition mp = null;
        try {
            mp = MountPartition.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        INodeKeyedObjects.getMoveCache().put(mp.getMountPoint(), mp.getNewUri());
    }

    @Override
    public long getProtocolVersion(String s, long l) throws IOException {
        return FSMountRepartitionProtocol.versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return new ProtocolSignature(FSMountRepartitionProtocol.versionID, null);
    }
}
