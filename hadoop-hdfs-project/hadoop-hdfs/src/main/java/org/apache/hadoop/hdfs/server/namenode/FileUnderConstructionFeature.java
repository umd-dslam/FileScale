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

import org.apache.hadoop.hdfs.db.*;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;

import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Feature for under-construction file.
 */
@InterfaceAudience.Private
public class FileUnderConstructionFeature implements INode.Feature {
  private String clientName;
  private String clientMachine;

  public FileUnderConstructionFeature() {} 

  public FileUnderConstructionFeature(final long id, final String clientName, final String clientMachine) {
    this.clientName = clientName;
    this.clientMachine = clientMachine;
    CompletableFuture.runAsync(() -> {
      DatabaseINode.insertUc(id, clientName, clientMachine);
    }, Database.getInstance().getExecutorService());
  }

  public void updateFileUnderConstruction(final String clientName, final String clientMachine) {
    this.clientName = clientName;
    this.clientMachine = clientMachine;
    // CompletableFuture.runAsync(() -> {
    //   DatabaseINode.insertUc(id, clientName, clientMachine);
    // }, Database.getInstance().getExecutorService());
  }

  public String getClientName(final long id) {
    if (this.clientName == null) {
      this.clientName = DatabaseINode.getUcClientName(id);
    }
    return this.clientName;
  }

  public void setClientName(final long id, String clientName) {
    this.clientName = clientName;
    // CompletableFuture.runAsync(() -> {
    //   DatabaseINode.setUcClientName(id, clientName);
    // }, Database.getInstance().getExecutorService());
  }

  public String getClientMachine(final long id) {
    if (this.clientMachine == null) {
      this.clientMachine = DatabaseINode.getUcClientMachine(id);
    }
    return this.clientMachine;
  }

  public void setClientMachine(final long id, String clientMachine) {
    this.clientMachine = clientMachine;
    // CompletableFuture.runAsync(() -> {
    //   DatabaseINode.setUcClientMachine(id, clientMachine);
    // }, Database.getInstance().getExecutorService());
  }

  @Override
  public boolean equals(Object o) {
    if ((o == null) || (o.getClass() != this.getClass())) {
      return false;
    }
    FileUnderConstructionFeature other = (FileUnderConstructionFeature) o;
    return new EqualsBuilder()
        .append(clientName, other.clientName)
        .append(clientMachine, other.clientMachine)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(this.clientName).append(this.clientMachine).toHashCode();
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
