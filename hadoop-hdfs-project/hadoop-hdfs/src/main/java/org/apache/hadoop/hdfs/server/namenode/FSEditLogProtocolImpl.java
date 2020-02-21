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

import org.apache.hadoop.ipc.VersionedProtocol;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;

import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;

import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.PermissionStatus;

import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields.PermissionStatusFormat;
import org.apache.hadoop.hdfs.server.namenode.AclEntryStatusFormat;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.LoaderContext;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.SaverContext;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FilesUnderConstructionSection.FileUnderConstructionEntry;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeDirectorySection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.AclFeatureProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.XAttrCompactProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.XAttrFeatureProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.QuotaByStorageTypeEntryProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.QuotaByStorageTypeFeatureProto;

import com.google.common.base.Preconditions;

public interface FSEditLogProtocolImpl extends FSEditLogProtocol {

    private INodeFile loadINodeFile(INodeSection.INode n) {
        assert n.getType() == INodeSection.INode.Type.FILE;
        INodeSection.INodeFile f = n.getFile();
        List<BlockProto> bp = f.getBlocksList();
        BlockType blockType = PBHelperClient.convert(f.getBlockType());
        boolean isStriped = f.hasErasureCodingPolicyID();
        assert ((!isStriped) || (isStriped && !f.hasReplication()));
        Short replication = (!isStriped ? (short) f.getReplication() : null);
        Byte ecPolicyID = (isStriped ?
            (byte) f.getErasureCodingPolicyID() : null);
        ErasureCodingPolicy ecPolicy = isStriped ?
            fsn.getErasureCodingPolicyManager().getByID(ecPolicyID) : null;
  
        BlockInfo[] blocks = new BlockInfo[bp.size()];
        for (int i = 0; i < bp.size(); ++i) {
            BlockProto b = bp.get(i);
            if (isStriped) {
                Preconditions.checkState(ecPolicy.getId() > 0,
                    "File with ID " + n.getId() +
                    " has an invalid erasure coding policy ID " + ecPolicy.getId());
                blocks[i] = new BlockInfoStriped(PBHelperClient.convert(b), ecPolicy);
            } else {
                blocks[i] = new BlockInfoContiguous(PBHelperClient.convert(b),
                    replication);
            }
        }
  
        final PermissionStatus permissions = PermissionStatusFormat.toPermissionStatus(
            f.getPermission(), null);
  
        final INodeFile file = new INodeFile(n.getId(),
            n.getName().toByteArray(), permissions, f.getModificationTime(),
            f.getAccessTime(), blocks, replication, ecPolicyID,
            f.getPreferredBlockSize(), (byte)f.getStoragePolicyID(), blockType);
  
        if (f.hasAcl()) {
            int[] entries = AclEntryStatusFormat.toInt(loadAclEntries(f.getAcl(), null));
            file.addAclFeature(new AclFeature(entries));
        }
  
        if (f.hasXAttrs()) {
            file.addXAttrFeature(new XAttrFeature(loadXAttrs(f.getXAttrs(), null)));
        }
  
        // under-construction information
        if (f.hasFileUC()) {
            INodeSection.FileUnderConstructionFeature uc = f.getFileUC();
            file.toUnderConstruction(uc.getClientName(), uc.getClientMachine());
            if (blocks.length > 0) {
                BlockInfo lastBlk = file.getLastBlock();
                // replace the last block of file
                final BlockInfo ucBlk;
                if (isStriped) {
                    BlockInfoStriped striped = (BlockInfoStriped) lastBlk;
                    ucBlk = new BlockInfoStriped(striped, ecPolicy);
                } else {
                    ucBlk = new BlockInfoContiguous(lastBlk,
                        replication);
                }
                ucBlk.convertToBlockUnderConstruction(
                    HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION, null);
                file.setBlock(file.numBlocks() - 1, ucBlk);
            }
        }
        return file;
    }

    private INodeDirectory loadINodeDirectory(INodeSection.INode n) {
      assert n.getType() == INodeSection.INode.Type.DIRECTORY;
      INodeSection.INodeDirectory d = n.getDirectory();

      final PermissionStatus permissions = loadPermission(d.getPermission(), null);
      final INodeDirectory dir = new INodeDirectory(n.getId(), n.getName()
          .toByteArray(), permissions, d.getModificationTime());
      final long nsQuota = d.getNsQuota(), dsQuota = d.getDsQuota();

      if (d.hasAcl()) {
        int[] entries = AclEntryStatusFormat.toInt(loadAclEntries(d.getAcl(), null));
        dir.addAclFeature(new AclFeature(entries));
      }
      if (d.hasXAttrs()) {
        dir.addXAttrFeature(new XAttrFeature(loadXAttrs(d.getXAttrs(), null)));
      }
      return dir;
    }

    @Override
    public void logEdit(byte[] in) throws IOException {
        INodeSection.INode p = INodeSection.INode.parseDelimitedFrom(in);
        INode n;
        switch (p.getType()) {
            case FILE:
                n = loadINodeFile(p);
                FSDirectory.getInstance().getEditLog().logOpenFile(null, n, true, false);
            case DIRECTORY:
                n = loadINodeDirectory(p, parent.getLoaderContext());
                FSDirectory.getInstance().getEditLog().logMkDir(null, n);
            default:
                break;
        }
    }

    @Override
    public long getProtocolVersion(String s, long l) throws IOException {
        return FSEditLogProtocol.versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return new ProtocolSignature(FSEditLogProtocol.versionID, null);
    }
}
