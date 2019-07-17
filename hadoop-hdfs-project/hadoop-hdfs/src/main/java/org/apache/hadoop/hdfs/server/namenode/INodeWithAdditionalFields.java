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

import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.LongBitFormat;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.db.*;
import org.apache.hadoop.util.LightWeightGSet.LinkedElement;

import com.google.common.base.Preconditions;

/**
 * {@link INode} with additional fields including id, name, permission,
 * access time and modification time.
 */
@InterfaceAudience.Private
public abstract class INodeWithAdditionalFields extends INode {
  // Note: this format is used both in-memory and on-disk.  Changes will be
  // incompatible.
  enum PermissionStatusFormat implements LongBitFormat.Enum {
    MODE(null, 16),
    GROUP(MODE.BITS, 24),
    USER(GROUP.BITS, 24);

    final LongBitFormat BITS;

    private PermissionStatusFormat(LongBitFormat previous, int length) {
      BITS = new LongBitFormat(name(), previous, length, 0);
    }

    static String getUser(long permission) {
      final int n = (int)USER.BITS.retrieve(permission);
      String s = SerialNumberManager.USER.getString(n);
      assert s != null;
      return s;
    }

    static String getGroup(long permission) {
      final int n = (int)GROUP.BITS.retrieve(permission);
      return SerialNumberManager.GROUP.getString(n);
    }
    
    static short getMode(long permission) {
      return (short)MODE.BITS.retrieve(permission);
    }

    /** Encode the {@link PermissionStatus} to a long. */
    static long toLong(PermissionStatus ps) {
      long permission = 0L;
      final int user = SerialNumberManager.USER.getSerialNumber(
          ps.getUserName());
      assert user != 0;
      permission = USER.BITS.combine(user, permission);
      // ideally should assert on group but inodes are created with null
      // group and then updated only when added to a directory.
      final int group = SerialNumberManager.GROUP.getSerialNumber(
          ps.getGroupName());
      permission = GROUP.BITS.combine(group, permission);
      final int mode = ps.getPermission().toShort();
      permission = MODE.BITS.combine(mode, permission);
      return permission;
    }

    static PermissionStatus toPermissionStatus(long id,
        SerialNumberManager.StringTable stringTable) {
      int uid = (int)USER.BITS.retrieve(id);
      int gid = (int)GROUP.BITS.retrieve(id);
      return new PermissionStatus(
          SerialNumberManager.USER.getString(uid, stringTable),
          SerialNumberManager.GROUP.getString(gid, stringTable),
          new FsPermission(getMode(id)));
    }

    @Override
    public int getLength() {
      return BITS.getLength();
    }
  }

  /** The inode id. */
  private long id;
  /**
   *  The inode name is in java UTF8 encoding; 
   *  The name in HdfsFileStatus should keep the same encoding as this.
   *  if this encoding is changed, implicitly getFileInfo and listStatus in
   *  clientProtocol are changed; The decoding at the client
   *  side should change accordingly.
   */
  private byte[] name = null;
  /** 
   * Permission encoded using {@link PermissionStatusFormat}.
   * Codes other than {@link #clonePermissionStatus(INodeWithAdditionalFields)}
   * and {@link #updatePermissionStatus(PermissionStatusFormat, long)}
   * should not modify it.
   */
  private long permission = 0L;
  private long modificationTime = -1L;
  private long accessTime = -1L;

  /** An array {@link Feature}s. */
  private static final Feature[] EMPTY_FEATURE = new Feature[0];
  // protected Feature[] features = EMPTY_FEATURE;

  private INodeWithAdditionalFields(INode parent, long id, byte[] name,
      long permission, long modificationTime, long accessTime, long header) {
    super(parent);
    this.id = id;
    this.name = name;
    this.permission = permission;
    this.modificationTime = modificationTime;
    this.accessTime = accessTime;

    INodeKeyedObjects.getBackupSet().add(id);
  }

  public void InitINodeWithAdditionalFields(INode parent, long id, byte[] name,
    long permission, long modificationTime, long accessTime, long header) {
    super.InitINode(parent);
    this.id = id;
    this.name = name;
    this.permission = permission;
    this.modificationTime = modificationTime;
    this.accessTime = accessTime;
  }

  public void InitINodeWithAdditionalFields(long id, byte[] name, PermissionStatus permissions,
    long modificationTime, long accessTime, long header, INodeDirectory parent) {
    InitINodeWithAdditionalFields(parent, id, name, PermissionStatusFormat.toLong(permissions),
        modificationTime, accessTime, header); 
  }

  public void updateINode(long header) {
    CompletableFuture.runAsync(() -> {
      DatabaseINode.insertInode(id,
      getParentId(),
      name != null && name.length > 0 ? DFSUtil.bytes2String(name) : null,
      accessTime, modificationTime, permission, header);
    }, Database.getInstance().getExecutorService());      
  }

  INodeWithAdditionalFields(long id, byte[] name, PermissionStatus permissions,
      long modificationTime, long accessTime, long header) {
    this(null, id, name, PermissionStatusFormat.toLong(permissions),
        modificationTime, accessTime, header);
  }

  INodeWithAdditionalFields(long id, byte[] name, PermissionStatus permissions,
      long modificationTime, long accessTime, long header, INodeDirectory parent) {
    this(parent, id, name, PermissionStatusFormat.toLong(permissions),
        modificationTime, accessTime, header);
  }

  public void InitINodeWithAdditionalFields(INode parent, long id, byte[] name, PermissionStatus permissions,
      long modificationTime, long accessTime) {
    InitINodeWithAdditionalFields(parent, id, name, PermissionStatusFormat.toLong(permissions),
        modificationTime, accessTime, 0L);
  }

  INodeWithAdditionalFields(INode parent, long id, byte[] name, PermissionStatus permissions,
      long modificationTime, long accessTime) {
    this(parent, id, name, PermissionStatusFormat.toLong(permissions),
        modificationTime, accessTime, 0L);
  }

  private INodeWithAdditionalFields(INode parent, long id) {
    super(parent);
    this.id = id;
  }

  // Note: only used by the loader of image file
  INodeWithAdditionalFields(long id) {
    this(null, id);
  }

  /** @param other Other node to be copied */
  INodeWithAdditionalFields(INodeWithAdditionalFields other) {
    this(other.getParentReference() != null ? other.getParentReference()
        : other.getParent(), other.getId(), other.getLocalNameBytes(),
          other.getPermissionLong(),
          other.getModificationTime(),
          other.getAccessTime(), 0L);
  }

  /** Get inode id */
  @Override
  public final long getId() {
    return this.id;
  }

  public final boolean isNameCached() {
    return name != null;
  }

  @Override
  public final byte[] getLocalNameBytes() {
    if (name == null) {
      String strName = DatabaseINode.getName(getId());
      name = (strName != null) ? DFSUtil.string2Bytes(strName) : null;
    }
    return name;
  }
  
  @Override
  public final void setLocalName(byte[] name) {
    if (name != null) {
      this.name = name;
    } else {
      this.name = null;
    }
  }

  /** Clone the {@link PermissionStatus}. */
  final void clonePermissionStatus(INodeWithAdditionalFields that) {
    permission = that.getPermissionLong();
  }

  @Override
  final PermissionStatus getPermissionStatus(int snapshotId) {
    return new PermissionStatus(getUserName(snapshotId), getGroupName(snapshotId),
        getFsPermission(snapshotId));
  }

  private final void setPermission(long perm) {
    permission = perm; 
  }

  private final void updatePermissionStatus(PermissionStatusFormat f, long n) {
    permission = f.BITS.combine(n, getPermissionLong());
  }

  @Override
  final String getUserName(int snapshotId) {
    if (snapshotId != Snapshot.CURRENT_STATE_ID) {
      return getSnapshotINode(snapshotId).getUserName();
    }
    return PermissionStatusFormat.getUser(getPermissionLong());
  }

  @Override
  final void setUser(String user) {
    int n = SerialNumberManager.USER.getSerialNumber(user);
    updatePermissionStatus(PermissionStatusFormat.USER, n);
  }

  @Override
  final String getGroupName(int snapshotId) {
    if (snapshotId != Snapshot.CURRENT_STATE_ID) {
      return getSnapshotINode(snapshotId).getGroupName();
    }
    return PermissionStatusFormat.getGroup(getPermissionLong());
  }

  @Override
  final void setGroup(String group) {
    int n = SerialNumberManager.GROUP.getSerialNumber(group);
    updatePermissionStatus(PermissionStatusFormat.GROUP, n);
  }

  @Override
  final FsPermission getFsPermission(int snapshotId) {
    if (snapshotId != Snapshot.CURRENT_STATE_ID) {
      return getSnapshotINode(snapshotId).getFsPermission();
    }

    return new FsPermission(getFsPermissionShort());
  }

  @Override
  public final short getFsPermissionShort() {
    return PermissionStatusFormat.getMode(getPermissionLong());
  }

  @Override
  void setPermission(FsPermission permission) {
    final short mode = permission.toShort();
    updatePermissionStatus(PermissionStatusFormat.MODE, mode);
  }

  @Override
  public long getPermissionLong() {
    if (permission == 0L) {
      permission = DatabaseINode.getPermission(getId()); 
    }
    return permission;
  }

  @Override
  public final AclFeature getAclFeature(int snapshotId) {
    if (snapshotId != Snapshot.CURRENT_STATE_ID) {
      return getSnapshotINode(snapshotId).getAclFeature();
    }

    return getFeature(AclFeature.class);
  }

  @Override
  final long getModificationTime(int snapshotId) {
    if (snapshotId != Snapshot.CURRENT_STATE_ID) {
      return getSnapshotINode(snapshotId).getModificationTime();
    }

    if (modificationTime == -1L) {
      modificationTime = DatabaseINode.getModificationTime(this.getId());
    }
    return modificationTime;
  }


  /** Update modification time if it is larger than the current value. */
  @Override
  public final INode updateModificationTime(long mtime, int latestSnapshotId) {
    Preconditions.checkState(isDirectory());
    if (mtime <= getModificationTime()) {
      return this;
    }
    return setModificationTime(mtime, latestSnapshotId);
  }

  final void cloneModificationTime(INodeWithAdditionalFields that) {
    modificationTime = that.getModificationTime(); 
  }

  @Override
  public final void setModificationTime(long modificationTime) {
    this.modificationTime = modificationTime;
  }

  @Override
  final long getAccessTime(int snapshotId) {
    if (snapshotId != Snapshot.CURRENT_STATE_ID) {
      return getSnapshotINode(snapshotId).getAccessTime();
    }

    if (accessTime == -1L) {
      accessTime = DatabaseINode.getAccessTime(this.getId());
    }
    return accessTime;
  }

  /**
   * Set last access time of inode.
   */
  @Override
  public final void setAccessTime(long accessTime) {
    this.accessTime = accessTime;
  }

  protected void addFeature(Feature f) {
    // int size = features.length;
    // Feature[] arr = new Feature[size + 1];
    // if (size != 0) {
    //   System.arraycopy(features, 0, arr, 0, size);
    // }
    // arr[size] = f;
    // features = arr;
  }

  protected void removeXAttrFeature(long id) {
    CompletableFuture.runAsync(() -> {
      DatabaseINode.removeXAttr(id);
    }, Database.getInstance().getExecutorService());
  }

  protected void removeFeature(Feature f) {
    // int size = features.length;
    // if (size == 0) {
    //   throwFeatureNotFoundException(f);
    // }

    // if (size == 1) {
    //   if (features[0] != f) {
    //     throwFeatureNotFoundException(f);
    //   }
    //   features = EMPTY_FEATURE;
    //   return;
    // }

    // Feature[] arr = new Feature[size - 1];
    // int j = 0;
    // boolean overflow = false;
    // for (Feature f1 : features) {
    //   if (f1 != f) {
    //     if (j == size - 1) {
    //       overflow = true;
    //       break;
    //     } else {
    //       arr[j++] = f1;
    //     }
    //   }
    // }

    // if (overflow || j != size - 1) {
    //   throwFeatureNotFoundException(f);
    // }
    // features = arr;
  }

  private void throwFeatureNotFoundException(Feature f) {
    throw new IllegalStateException(
        "Feature " + f.getClass().getSimpleName() + " not found.");
  }

  protected <T extends Feature> T getFeature(Class<? extends Feature> clazz) {
    Preconditions.checkArgument(clazz != null);
    // final int size = features.length;
    // for (int i=0; i < size; i++) {
    //   Feature f = features[i];
    //   if (clazz.isAssignableFrom(f.getClass())) {
    //     @SuppressWarnings("unchecked")
    //     T ret = (T) f;
    //     return ret;
    //   }
    // }
    return null;
  }

  public void removeAclFeature() {
    AclFeature f = getAclFeature();
    Preconditions.checkNotNull(f);
    removeFeature(f);
    AclStorage.removeAclFeature(f);
  }

  public void addAclFeature(AclFeature f) {
    AclFeature f1 = getAclFeature();
    if (f1 != null)
      throw new IllegalStateException("Duplicated ACLFeature");

    addFeature(AclStorage.addAclFeature(f));
  }
  
  @Override
  XAttrFeature getXAttrFeature(int snapshotId) {
    if (snapshotId != Snapshot.CURRENT_STATE_ID) {
      return getSnapshotINode(snapshotId).getXAttrFeature();
    }
    // FIXME: disable XAttr
    // if(!XAttrFeature.isFileXAttr(getId())) {
    //   return null;
    // }
    // return new XAttrFeature(getId());

    return null;
  }
  
  @Override
  public void removeXAttrFeature() {
    removeXAttrFeature(getId());
  }
  
  @Override
  public void addXAttrFeature(XAttrFeature f) {
    if (f.getId() != getId()) {
      Preconditions.checkState(!XAttrFeature.isFileXAttr(getId()), "Duplicated XAttrFeature");
      XAttrFeature.createXAttrFeature(getId(), f.getXAttrs());
    }
  }

  public final Feature[] getFeatures() {
    // return features;
    return EMPTY_FEATURE;
  }
}
