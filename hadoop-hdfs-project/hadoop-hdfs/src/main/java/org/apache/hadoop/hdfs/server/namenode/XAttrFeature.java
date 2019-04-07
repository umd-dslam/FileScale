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

import com.google.common.collect.ImmutableList;
import com.google.common.base.Preconditions;

/**
 * Feature for extended attributes.
 */
@InterfaceAudience.Private
public class XAttrFeature implements INode.Feature {

  private static XAttrFeature instance; 

  public static XAttrFeature getInstance() {
    if (instance == null) {
      instance = new XAttrFeature();
    }
    return instance;
  }

  public XAttrFeature() {}

  public XAttrFeature(long id, List<XAttr> xAttrs) {
    Preconditions.checkState(!isFileXAttr(id), "Duplicated XAttrFeature");
    List<Long> ids = new ArrayList<Long>();
    if (xAttrs != null && !xAttrs.isEmpty()) {
      for (XAttr attr : xAttrs) {
         ids.add(attr.getId());
      }
      DatabaseINode.insertXAttrs(id, ids);
    }
  }

  public static createXAttrFeature(long id, List<XAttr> xAttr) {
    Preconditions.checkState(!isFileXAttr(id), "Duplicated XAttrFeature");
    List<Long> ids = new ArrayList<Long>();
    if (xAttrs != null && !xAttrs.isEmpty()) {
      for (XAttr attr : xAttrs) {
         ids.add(attr.getId());
      }
      DatabaseINode.insertXAttrs(id, ids);
    }
  }

  public static Boolean isFileXAttr(long id) {
    return DatabaseINode.checkXAttrExistence(id);
  }

  /**
   * Get the XAttrs.
   * @return the XAttrs
   */
  public static List<XAttr> getXAttrs(long id) {
    return DatabaseINode.getXAttrs(id);
  }

  /**
   * Get XAttr by name with prefix.
   * @param prefixedName xAttr name with prefix
   * @return the XAttr
   */
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
