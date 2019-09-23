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
import org.apache.hadoop.hdfs.db.*;

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

  /**
   * Get the {@link INode} with the given id from the map.
   *
   * @param id ID of the {@link INode}.
   * @return The {@link INode} in the map with the given id. Return null if no such {@link INode} in
   *     the map.
   */
  public INode get(long id) {
    INode inode = INodeKeyedObjects.getCache().getIfPresent(Long.class, id);
    if (inode == null) {
      DatabaseINode.LoadINode node = new DatabaseINode().loadINode(id);
      byte[] name = (node.name != null) ? DFSUtil.string2Bytes(node.name) : null;
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
                node.header);
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
                node.header);
      }
      INodeKeyedObjects.getCache()
          .put(
              new CompositeKey((Long) node.id, new ImmutablePair<>((Long) node.parent, node.name)),
              inode);
    }
    return inode;
  }

  public INode get(long parentId, String childName) {
    INode inode =
        INodeKeyedObjects.getCache()
            .getIfPresent(Pair.class, new ImmutablePair<>((Long) parentId, childName));
    if (inode == null) {
      DatabaseINode.LoadINode node = new DatabaseINode().loadINode(parentId, childName);
      byte[] name = (node.name != null) ? DFSUtil.string2Bytes(node.name) : null;
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
                node.header);
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
                node.header);
      }
      INodeKeyedObjects.getCache()
          .put(new CompositeKey((Long) node.id, new ImmutablePair<>(parentId, childName)), inode);
    }
    return inode;
  }

  public boolean find(long id) {
    if (INodeKeyedObjects.getCache().getIfPresent(Long.class, id) != null
        || DatabaseINode.checkInodeExistence(id)) {
      return true;
    }
    return false;
  }

  public void clear() {}
}
