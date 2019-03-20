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

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.LightWeightGSet;

import org.apache.hadoop.hdfs.db.*;

/**
 * This class maintains the map from a block to its metadata.
 * block's metadata currently includes blockCollection it belongs to and
 * the datanodes that store the block.
 */
class BlocksMap {

  private final LongAdder totalReplicatedBlocks = new LongAdder();
  private final LongAdder totalECBlockGroups = new LongAdder();

  BlocksMap() {}

  void close() {
    clear();
  }
  
  void clear() {
    totalReplicatedBlocks.reset();
    totalECBlockGroups.reset();
  }

  /**
   * Add block b belonging to the specified block collection to the map.
   */
  BlockInfo addBlockCollection(BlockInfo b, BlockCollection bc) {
    incrementBlockStat(b);
    return b;
  }

  /**
   * Remove the block from the block map;
   * remove it from all data-node lists it belongs to;
   * and remove all data-node locations associated with the block.
   */
  void removeBlock(BlockInfo block) {
    if (block == null) {
      return;
    }
    decrementBlockStat(block);

    assert block.getBlockCollectionId() == 0;
    final int size = block.isStriped() ?
        block.getCapacity() : block.numNodes();
    for(int idx = size - 1; idx >= 0; idx--) {
      DatanodeDescriptor dn = block.getDatanode(idx);
      if (dn != null) {
        removeBlock(dn, block); // remove from the list and wipe the location
      }
    }
    DatabaseDatablock.delete(block.getBlockId());
  }

  /**
   * Check if BlocksMap contains the block.
   *
   * @param b Block to check
   * @return true if block is in the map, otherwise false
   */
  boolean containsBlock(Block b) {
    return DatabaseINode2Block.getBcId(b.getBlockId()) == 0 ? false : true;
  }

  /** Returns the block object if it exists in the map. */
  BlockInfo getStoredBlock(Block b) {
    if (containsBlock(b)) {
      return new BlockInfo(b);
    } else {
      return null;
    }
  }

  /**
   * Searches for the block in the BlocksMap and 
   * returns {@link Iterable} of the storages the block belongs to.
   */
  Iterable<DatanodeStorageInfo> getStorages(Block b) {
    BlockInfo info = getStoredBlock(b);
    return info != null ? getStorages(info)
           : Collections.<DatanodeStorageInfo>emptyList();
  }

  /**
   * For a block that has already been retrieved from the BlocksMap
   * returns {@link Iterable} of the storages the block belongs to.
   */
  Iterable<DatanodeStorageInfo> getStorages(final BlockInfo storedBlock) {
    if (storedBlock == null) {
      return Collections.emptyList();
    } else {
      return new Iterable<DatanodeStorageInfo>() {
        @Override
        public Iterator<DatanodeStorageInfo> iterator() {
          return storedBlock.getStorageInfos();
        }
      };
    }
  }

  /** counts number of containing nodes. Better than using iterator. */
  int numNodes(Block b) {
    BlockInfo info = getStoredBlock(b);
    return info == null ? 0 : info.numNodes();
  }

  /**
   * Remove data-node reference from the block.
   * Remove the block from the block map
   * only if it does not belong to any file and data-nodes.
   */
  boolean removeNode(Block b, DatanodeDescriptor node) {
    BlockInfo info = getStoredBlock(b);
    if (info == null)
      return false;

    // remove block from the data-node set and the node from the block info
    boolean removed = removeBlock(node, info);

    if (info.hasNoStorage()    // no datanodes left
        && info.isDeleted()) { // does not belong to a file
      DatabaseDatablock.delete(b.getBlockId()); 
      decrementBlockStat(info);
    }
    return removed;
  }

  /**
   * Remove block from the set of blocks belonging to the data-node. Remove
   * data-node from the block.
   */
  static boolean removeBlock(DatanodeDescriptor dn, BlockInfo b) {
    final DatanodeStorageInfo s = b.findStorageInfo(dn);
    // if block exists on this datanode
    return s != null && s.removeBlock(b);
  }

  long size() {
    return DatabaseINode2Block.getSize();
  }

  private void incrementBlockStat(BlockInfo block) {
    if (block.isStriped()) {
      totalECBlockGroups.increment();
    } else {
      totalReplicatedBlocks.increment();
    }
  }

  private void decrementBlockStat(BlockInfo block) {
    if (block.isStriped()) {
      totalECBlockGroups.decrement();
      assert totalECBlockGroups.longValue() >= 0 :
          "Total number of ec block groups should be non-negative";
    } else {
      totalReplicatedBlocks.decrement();
      assert totalReplicatedBlocks.longValue() >= 0 :
          "Total number of replicated blocks should be non-negative";
    }
  }

  long getReplicatedBlocks() {
    return totalReplicatedBlocks.longValue();
  }

  long getECBlockGroups() {
    return totalECBlockGroups.longValue();
  }
}
