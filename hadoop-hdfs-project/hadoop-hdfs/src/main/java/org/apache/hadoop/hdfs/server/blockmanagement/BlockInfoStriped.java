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
package org.apache.hadoop.hdfs.server.blockmanagement;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.db.*;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Subclass of {@link BlockInfo}, presenting a block group in erasure coding.
 *
 * We still use a storage array to store DatanodeStorageInfo for each block in
 * the block group. For a (m+k) block group, the first (m+k) storage units
 * are sorted and strictly mapped to the corresponding block.
 *
 * Normally each block belonging to group is stored in only one DataNode.
 * However, it is possible that some block is over-replicated. Thus the storage
 * array's size can be larger than (m+k). Thus currently we use an extra byte
 * array to record the block index for each entry.
 */
@InterfaceAudience.Private
public class BlockInfoStriped extends BlockInfo {
  public BlockInfoStriped(Block blk, ErasureCodingPolicy ecPolicy) {
    super(blk, (short) (ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits()));
    DatabaseDatablock.setECPolicyId(blk.getBlockId(), ecPolicy.getId();
  }

  public byte getECPolicyId() {
    return DatabaseDatablock.getECPolicyId(blk.getBlockId());
  }

  public short getTotalBlockNum() {
    ErasureCodingPolicy ecPolicy = getErasureCodingPolicy();
    return (short) (ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits());
  }

  public short getDataBlockNum() {
    ErasureCodingPolicy ecPolicy = getErasureCodingPolicy();
    return (short) ecPolicy.getNumDataUnits();
  }

  public short getParityBlockNum() {
    ErasureCodingPolicy ecPolicy = getErasureCodingPolicy();
    return (short) ecPolicy.getNumParityUnits();
  }

  public int getCellSize() {
    ErasureCodingPolicy ecPolicy = getErasureCodingPolicy();
    return ecPolicy.getCellSize();
  }

  /**
   * If the block is committed/completed and its length is less than a full
   * stripe, it returns the the number of actual data blocks.
   * Otherwise it returns the number of data units specified by erasure coding policy.
   */
  public short getRealDataBlockNum() {
    if (isComplete() || getBlockUCState() == BlockUCState.COMMITTED) {
      return (short) Math.min(getDataBlockNum(),
          (getNumBytes() - 1) / getCellSize() + 1);
    } else {
      return getDataBlockNum();
    }
  }

  public short getRealTotalBlockNum() {
    return (short) (getRealDataBlockNum() + getParityBlockNum());
  }

  public ErasureCodingPolicy getErasureCodingPolicy() {
    return ErasureCodingPolicyManager.getInstance().getByID(getECPolicyId());
  }

  private int findSlot() {
    int i = getTotalBlockNum();
    for (; i < getCapacity(); i++) {
      if (getStorageInfo(i) == null) {
        return i;
      }
    }
    // need to expand the storage size
    ensureCapacity(i + 1, true);
    return i;
  }

  @Override
  boolean addStorage(DatanodeStorageInfo storage, Block reportedBlock) {
    Preconditions.checkArgument(BlockIdManager.isStripedBlockID(
        reportedBlock.getBlockId()), "reportedBlock is not striped");
    Preconditions.checkArgument(BlockIdManager.convertToStripedID(
        reportedBlock.getBlockId()) == this.getBlockId(),
        "reported blk_%s does not belong to the group of stored blk_%s",
        reportedBlock.getBlockId(), this.getBlockId());
    int blockIndex = BlockIdManager.getBlockIndex(reportedBlock);
    int index = blockIndex;
    DatanodeStorageInfo old = getStorageInfo(index);
    if (old != null && !old.equals(storage)) { // over replicated
      // check if the storage has been stored
      int i = findStorageInfo(storage);
      if (i == -1) {
        index = findSlot();
      } else {
        return true;
      }
    }
    addStorage(storage, index, blockIndex);
    return true;
  }

  private void addStorage(DatanodeStorageInfo storage, int index,
      int blockIndex) {
    setStorageInfo(index, storage);
    DatabaseDatablock.addStorage(getBlockId(), index, (byte) blockIndex);
  }

  private int findStorageInfoFromEnd(DatanodeStorageInfo storage) {
    final int len = getCapacity();
    for(int idx = len - 1; idx >= 0; idx--) {
      DatanodeStorageInfo cur = getStorageInfo(idx);
      if (storage.equals(cur)) {
        return idx;
      }
    }
    return -1;
  }

  byte getStorageBlockIndex(DatanodeStorageInfo storage) {
    int i = this.findStorageInfo(storage);
    return i == -1 ? -1 : DatabaseDatablock.getStorageBlockIndex(getBlockId(), i);
  }

  /**
   * Identify the block stored in the given datanode storage. Note that
   * the returned block has the same block Id with the one seen/reported by the
   * DataNode.
   */
  Block getBlockOnStorage(DatanodeStorageInfo storage) {
    int index = getStorageBlockIndex(storage);
    if (index < 0) {
      return null;
    } else {
      long blkId = this.getBlockId();
      Long[] res = DatabaseDatablock.getNumBytesAndStamp(blkId);
      return new Block(blkId + index, res[0], res[1]);
    }
  }

  @Override
  boolean removeStorage(DatanodeStorageInfo storage) {
    int dnIndex = findStorageInfoFromEnd(storage);
    if (dnIndex < 0) { // the node is not found
      return false;
    }
    // set the entry to null
    setStorageInfo(dnIndex, null);
    DatabaseDatablock.setStorageBlockIndex(getBlockId(), dnIndex, -1);
    return true;
  }

  private void ensureCapacity(int totalSize, boolean keepOld) {
    if (getCapacity() < totalSize) {
      DatanodeStorageInfo[] old = storages;
      storages = new DatanodeStorageInfo[totalSize];

      if (keepOld) {
        System.arraycopy(old, 0, storages, 0, old.length);
      }
    }
  }

  public long spaceConsumed() {
    // In case striped blocks, total usage by this striped blocks should
    // be the total of data blocks and parity blocks because
    // `getNumBytes` is the total of actual data block size.
    return StripedBlockUtil.spaceConsumedByStripedBlock(getNumBytes(),
        getDataBlockNum(), getParityBlockNum(), getCellSize());
    }

  @Override
  public final boolean isStriped() {
    return true;
  }

  @Override
  public BlockType getBlockType() {
    return BlockType.STRIPED;
  }

  @Override
  public int numNodes() {
    assert this.storages != null : "BlockInfo is not initialized";
    int num = 0;
    for (int idx = getCapacity()-1; idx >= 0; idx--) {
      if (getStorageInfo(idx) != null) {
        num++;
      }
    }
    return num;
  }

  @Override
  final boolean hasNoStorage() {
    final int len = getCapacity();
    for(int idx = 0; idx < len; idx++) {
      if (getStorageInfo(idx) != null) {
        return false;
      }
    }
    return true;
  }

  /**
   * This class contains datanode storage information and block index in the
   * block group.
   */
  public static class StorageAndBlockIndex {
    private final DatanodeStorageInfo storage;
    private final byte blockIndex;

    StorageAndBlockIndex(DatanodeStorageInfo storage, byte blockIndex) {
      this.storage = storage;
      this.blockIndex = blockIndex;
    }

    /**
     * @return storage in the datanode.
     */
    public DatanodeStorageInfo getStorage() {
      return storage;
    }

    /**
     * @return block index in the block group.
     */
    public byte getBlockIndex() {
      return blockIndex;
    }
  }

  public Iterable<StorageAndBlockIndex> getStorageAndIndexInfos() {
    return new Iterable<StorageAndBlockIndex>() {
      @Override
      public Iterator<StorageAndBlockIndex> iterator() {
        return new Iterator<StorageAndBlockIndex>() {
          private int index = 0;

          @Override
          public boolean hasNext() {
            while (index < getCapacity() && getStorageInfo(index) == null) {
              index++;
            }
            return index < getCapacity();
          }

          @Override
          public StorageAndBlockIndex next() {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            int i = index++;
            return new StorageAndBlockIndex(storages[i], indices[i]);
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException("Remove is not supported");
          }
        };
      }
    };
  }
}
