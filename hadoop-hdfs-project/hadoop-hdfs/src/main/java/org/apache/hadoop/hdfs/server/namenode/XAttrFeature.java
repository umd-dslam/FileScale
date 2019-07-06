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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.db.*;

import com.google.common.collect.ImmutableList;
import com.google.common.base.Preconditions;

import java.util.concurrent.CompletableFuture;

/**
 * Feature for extended attributes.
 */
@InterfaceAudience.Private
public class XAttrFeature implements INode.Feature {
  private long id;

  public XAttrFeature(long id) { this.id = id; }

  public XAttrFeature(long id, List<XAttr> xAttrs) {
    createXAttrFeature(id, xAttrs);
    this.id = id;
  }

  public static void createXAttrFeature(long id, List<XAttr> xAttrs) {
    Preconditions.checkState(!isFileXAttr(id), "Duplicated XAttrFeature");
    List<Long> ids = new ArrayList<Long>();
    if (xAttrs != null && !xAttrs.isEmpty()) {
      List<Integer> ns = new ArrayList<Integer>();
      List<String> namevals = new ArrayList<String>();
      for (XAttr attr : xAttrs) {
        ns.add(attr.getNameSpace().ordinal());
        namevals.add(attr.getName());
        namevals.add(XAttr.bytes2String(attr.getValue()));
      }
      CompletableFuture.runAsync(() -> {
        DatabaseINode.insertXAttrs(id, ns, namevals);
      }, Database.getInstance().getExecutorService());
    }
  }

  public long getId() {
    return this.id;
  }

  public Boolean isFileXAttr() {
    return isFileXAttr(id);
  }

  public static Boolean isFileXAttr(long id) {
    return DatabaseINode.checkXAttrExistence(id);
  }

  /**
   * Get the XAttrs.
   * @return the XAttrs
   */
  public List<XAttr> getXAttrs() {
    return getXAttrs(id);
  }

  public static List<XAttr> getXAttrs(long id) {
    List<XAttr> xattrs = new ArrayList<XAttr>();
    List<DatabaseINode.XAttrInfo> xinfo = new DatabaseINode().getXAttrs(id);
    for (int i = 0; i < xinfo.size(); ++i) {
      xattrs.add(new XAttr(XAttr.NameSpace.values()[xinfo.get(i).getNameSpace()],
        xinfo.get(i).getName(), XAttr.string2Bytes(xinfo.get(i).getValue())));
    }
    return xattrs;
  }

  /**
   * Get XAttr by name with prefix.
   * @param prefixedName xAttr name with prefix
   * @return the XAttr
   */
  public XAttr getXAttr(String prefixedName) {
    return getXAttr(id, prefixedName);
  }

  public static XAttr getXAttr(long id, String prefixedName) {
    XAttr attr = null;
    XAttr toFind = XAttrHelper.buildXAttr(prefixedName);
    List<XAttr> xAttrs = getXAttrs(id);
    for (XAttr a : xAttrs) {
      if (a.equalsIgnoreValue(toFind)) {
        attr = a;
        break;
      }
    }
    return attr;
  }
}
