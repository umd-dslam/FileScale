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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;

/**
 * Feature for under-construction file.
 */
@InterfaceAudience.Private
public class FileUnderConstructionFeature implements INode.Feature {

  private static FileUnderConstructionFeature instance; 

  public static FileUnderConstructionFeature getInstance() {
    if (instance == null) {
      instance = new FileUnderConstructionFeature();
    }
    return instance;
  }

  public FileUnderConstructionFeature() {}

  public FileUnderConstructionFeature(final long id, final String clientName, final String clientMachine) {
    DatabaseINode.insertUc(id, clientName, clientMachine);
  }

  public static void createFileUnderConstruction(final long id, final String clientName, final String clientMachine) {
    DatabaseINode.insertUc(id, clientName, clientMachine);
  }

  public static Boolean isFileUnderConstruction(final long id) {
    return DatabaseINode.checkUCExistence(id);
  }

  public static String getClientName(final long id) {
    return DatabaseINode.getUcClientName(id);
  }

  public static void setClientName(final long id, String clientName) {
    DatabaseINode.setUcClientName(id, clientName);
  }

  public static String getClientMachine(final long id) {
    return DatabaseINode.getUcClientMachine(id);
  }

  /**
   * Update the length for the last block
   *
   * @param lastBlockLength
   *          The length of the last block reported from client
   * @throws IOException
   */
  static void updateLengthOfLastBlock(INodeFile f, long lastBlockLength)
      throws IOException {
    BlockInfo lastBlock = f.getLastBlock();
    assert (lastBlock != null) : "The last block for path "
        + f.getFullPathName() + " is null when updating its length";
    assert !lastBlock.isComplete()
        : "The last block for path " + f.getFullPathName()
            + " is not under-construction when updating its length";
    lastBlock.setNumBytes(lastBlockLength);
  }

  /**
   * When deleting a file in the current fs directory, and the file is contained
   * in a snapshot, we should delete the last block if it's under construction
   * and its size is 0.
   */
  static void cleanZeroSizeBlock(final INodeFile f,
      final BlocksMapUpdateInfo collectedBlocks) {
    final BlockInfo[] blocks = f.getBlocks();
    if (blocks != null && blocks.length > 0
        && !blocks[blocks.length - 1].isComplete()) {
      BlockInfo lastUC = blocks[blocks.length - 1];
      if (lastUC.getNumBytes() == 0) {
        // this is a 0-sized block. do not need check its UC state here
        collectedBlocks.addDeleteBlock(lastUC);
        f.removeLastBlock(lastUC);
      }
    }
  }
}
