/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.db.*;
import org.apache.hadoop.hdfs.cuckoofilter4j.*;

/** Storing all the {@link INode}s and maintaining the mapping between INode ID and INode. */
public class INodeMap {
  public INodeMap() {}

  /**
   * Add an {@link INode} into the {@link INode} map. Replace the old value if necessary.
   *
   * @param inode The {@link INode} to be added to the map.
   */
  public final void put(INode inode) {
    // already in inodes table
  }

  /**
   * Remove a {@link INode} from the map.
   *
   * @param inode The {@link INode} to be removed.
   */
  public final void remove(INode inode) {
    // TODO: double check where to delete inode from inodes table
  }

  /** @return The size of the map. */
  public long size() {
    return DatabaseINode.getINodesNum();
  }


  public INode get(String parentName, String childName) {
    INode inode = INodeKeyedObjects.getCache().getIfPresent(parentName + childName);
    if (inode == null) {
      INodeDirectory parent = INodeKeyedObjects.getCache().getIfPresent(parentName).asDirectory();
      if (!parent.filter.mightContain(String.valueOf(parent.getId()) + childName)) {
        return null;
      }
      DatabaseINode.LoadINode node = new DatabaseINode().loadINode(parent.getId(), childName);
      if (node == null) return null;
      byte[] name = (node.name != null && node.name.length() > 0) ? DFSUtil.string2Bytes(node.name) : null;
      if (node.header != 0L) {
        inode = new INodeFile(node.id);
        inode.asFile().setNumBlocks();
        inode
            .asFile()
            .InitINodeFile(
                node.parent,
                node.id,
                name,
                node.permission,
                node.modificationTime,
                node.accessTime,
                node.header,
                node.parentName);
      } else {
        inode = new INodeDirectory(node.id);
        inode
            .asDirectory()
            .InitINodeDirectory(
                node.parent,
                node.id,
                name,
                node.permission,
                node.modificationTime,
                node.accessTime,
                node.header,
                node.parentName);
      }
      INodeKeyedObjects.getCache()
          .put(parentName + childName, inode);
    }
    return inode;
  }


  public boolean find(INodeFile file) {
    if (INodeKeyedObjects.getCache().getIfPresent(file.getPath()) != null) {
      return true;
    }

    INodeDirectory parent = file.getParent();
    if (parent.filter.mightContain(String.valueOf(parent.getId()) + file.getLocalName())) {
      return true;
    }

    return false;
  }

  public void clear() {}
}
