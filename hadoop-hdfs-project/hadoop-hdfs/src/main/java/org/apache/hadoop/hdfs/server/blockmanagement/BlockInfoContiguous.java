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

/**
 * Subclass of {@link BlockInfo}, used for a block with replication scheme.
 */
@InterfaceAudience.Private
public class BlockInfoContiguous extends BlockInfo {

  public BlockInfoContiguous(short size) {
    super(size);
  }

  public BlockInfoContiguous(Block blk, short size) {
    super(blk, size);
  }

  public BlockInfoContiguous(long bid, long num, long stamp, short size) {
    super(bid, num, stamp, size);
  }

  @Override
  boolean addStorage(DatanodeStorageInfo storage, Block reportedBlock) {
    Preconditions.checkArgument(this.getBlockId() == reportedBlock.getBlockId(),
        "reported blk_%s is different from stored blk_%s",
        reportedBlock.getBlockId(), this.getBlockId());
    // find the last null node
    setStorageInfo(numNodes(), storage);
    return true;
  }

  @Override
  boolean removeStorage(DatanodeStorageInfo storage) {
    int dnIndex = findStorageInfo(storage);
    if (dnIndex < 0) { // the node is not found
      return false;
    }
    // find the last not null node
    int lastNode = numNodes()-1;
    // replace current node entry by the lastNode one
    setStorageInfo(dnIndex, getStorageInfo(lastNode));
    // set the last entry to null
    setStorageInfo(lastNode, null);
    return true;
  }

  @Override
  public int numNodes() {
    for (int idx = getCapacity()-1; idx >= 0; idx--) {
      if (getDatanode(idx) != null) {
        return idx + 1;
      }
    }
    return 0;
  }

  @Override
  public final boolean isStriped() {
    return false;
  }

  @Override
  public BlockType getBlockType() {
    return BlockType.CONTIGUOUS;
  }

  @Override
  final boolean hasNoStorage() {
    return getStorageInfo(0) == null;
  }
}
