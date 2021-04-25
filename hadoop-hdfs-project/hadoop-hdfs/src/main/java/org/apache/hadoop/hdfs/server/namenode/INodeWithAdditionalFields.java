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

import java.io.File;
import java.util.*;
import com.google.common.base.Preconditions;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.db.*;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.LongBitFormat;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

/**
 * {@link INode} with additional fields including id, name, permission, access time and modification
 * time.
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
      final int n = (int) USER.BITS.retrieve(permission);
      String s = SerialNumberManager.USER.getString(n);
      assert s != null;
      return s;
    }

    static String getGroup(long permission) {
      final int n = (int) GROUP.BITS.retrieve(permission);
      return SerialNumberManager.GROUP.getString(n);
    }

    static short getMode(long permission) {
      return (short) MODE.BITS.retrieve(permission);
    }

    /** Encode the {@link PermissionStatus} to a long. */
    static long toLong(PermissionStatus ps) {
      long permission = 0L;
      final int user = SerialNumberManager.USER.getSerialNumber(ps.getUserName());
      assert user != 0;
      permission = USER.BITS.combine(user, permission);
      // ideally should assert on group but inodes are created with null
      // group and then updated only when added to a directory.
      final int group = SerialNumberManager.GROUP.getSerialNumber(ps.getGroupName());
      permission = GROUP.BITS.combine(group, permission);
      final int mode = ps.getPermission().toShort();
      permission = MODE.BITS.combine(mode, permission);
      return permission;
    }

    static PermissionStatus toPermissionStatus(
        long id, SerialNumberManager.StringTable stringTable) {
      int uid = (int) USER.BITS.retrieve(id);
      int gid = (int) GROUP.BITS.retrieve(id);
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
   * The inode name is in java UTF8 encoding; The name in HdfsFileStatus should keep the same
   * encoding as this. if this encoding is changed, implicitly getFileInfo and listStatus in
   * clientProtocol are changed; The decoding at the client side should change accordingly.
   */
  private byte[] name = null;
  /**
   * Permission encoded using {@link PermissionStatusFormat}. Codes other than {@link
   * #clonePermissionStatus(INodeWithAdditionalFields)} and {@link
   * #updatePermissionStatus(PermissionStatusFormat, long)} should not modify it.
   */
  private long permission = -1L;

  private long modificationTime = -1L;
  private long accessTime = -1L;

  /** An array {@link Feature}s. */
  private static final Feature[] EMPTY_FEATURE = new Feature[0];
  // protected Feature[] features = EMPTY_FEATURE;

  private INodeWithAdditionalFields(
      INode parent,
      long id,
      byte[] name,
      long permission,
      long modificationTime,
      long accessTime,
      long header,
      String parentName) {
    super(parent, parentName);
    this.id = id;
    this.name = name;
    this.permission = permission;
    this.modificationTime = modificationTime;
    this.accessTime = accessTime;

    INodeKeyedObjects.getUpdateSet().add(this.getPath());
  }

  public void InitINodeWithAdditionalFields(
      long parent,
      long id,
      byte[] name,
      long permission,
      long modificationTime,
      long accessTime,
      long header,
      String parentName) {
    super.setParent(parent);
    super.setParentName(parentName);
    this.id = id;
    this.name = name;
    this.permission = permission;
    this.modificationTime = modificationTime;
    this.accessTime = accessTime;
  }

  public void InitINodeWithAdditionalFields(
      INode parent,
      long id,
      byte[] name,
      long permission,
      long modificationTime,
      long accessTime,
      long header,
      String parentName) {
    super.InitINode(parent);
    super.setParentName(parentName);
    this.id = id;
    this.name = name;
    this.permission = permission;
    this.modificationTime = modificationTime;
    this.accessTime = accessTime;
  }

  public void InitINodeWithAdditionalFields(
      long id,
      byte[] name,
      PermissionStatus permissions,
      long modificationTime,
      long accessTime,
      long header,
      INodeDirectory parent,
      String parentName) {
    InitINodeWithAdditionalFields(
        parent,
        id,
        name,
        PermissionStatusFormat.toLong(permissions),
        modificationTime,
        accessTime,
        header, parentName);
  }

  public void updateINode(long header) {
    CompletableFuture.runAsync(() -> {
    DatabaseINode.insertInode(
        id,
        getParentId(),
        name != null && name.length > 0 ? DFSUtil.bytes2String(name) : null,
        accessTime,
        modificationTime,
        permission,
        header,
        getParentName());
    }, Database.getInstance().getExecutorService());
  }

  INodeWithAdditionalFields(
      long id,
      byte[] name,
      PermissionStatus permissions,
      long modificationTime,
      long accessTime,
      long header,
      String parentName) {
    this(
        null,
        id,
        name,
        PermissionStatusFormat.toLong(permissions),
        modificationTime,
        accessTime,
        header,
        parentName);
  }

  INodeWithAdditionalFields(
      long id,
      byte[] name,
      PermissionStatus permissions,
      long modificationTime,
      long accessTime,
      long header,
      INodeDirectory parent,
      String parentName) {
    this(
        parent,
        id,
        name,
        PermissionStatusFormat.toLong(permissions),
        modificationTime,
        accessTime,
        header,
        parentName);
  }

  public void InitINodeWithAdditionalFields(
      INode parent,
      long id,
      byte[] name,
      PermissionStatus permissions,
      long modificationTime,
      long accessTime,
      String parentName) {
    InitINodeWithAdditionalFields(
        parent,
        id,
        name,
        PermissionStatusFormat.toLong(permissions),
        modificationTime,
        accessTime,
        0L,
        parentName);
  }

  INodeWithAdditionalFields(
      INode parent,
      long id,
      byte[] name,
      PermissionStatus permissions,
      long modificationTime,
      long accessTime,
      String parentName) {
    this(
        parent,
        id,
        name,
        PermissionStatusFormat.toLong(permissions),
        modificationTime,
        accessTime,
        0L,
        parentName);
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
    this(
        other.getParentReference() != null ? other.getParentReference() : other.getParent(),
        other.getId(),
        other.getLocalNameBytes(),
        other.getPermissionLong(),
        other.getModificationTime(),
        other.getAccessTime(),
        0L,
        other.getParentName());
  }

  /** Get inode id */
  @Override
  public final long getId() {
    return this.id;
  }


  @Override
  public void setId(Long id) {
    this.id = id;
  }

  public final boolean isNameCached() {
    return name != null;
  }

  @Override
  public final String getPath() {
    String path = null;
    if (getParentName().equals("/")) {
      path = getParentName() + getLocalName();
    } else {
      path = getParentName() + "/" + getLocalName();
    }
    return path;
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
    return new PermissionStatus(
        getUserName(snapshotId), getGroupName(snapshotId), getFsPermission(snapshotId));
  }

  private final void setPermission(long perm) {
    permission = perm;
    INodeKeyedObjects.getUpdateSet().add(getPath());
  }

  void update_subtree(Set<INode> inodes) {
    List<Long> longAttr = new ArrayList<>();
    List<String> strAttr = new ArrayList<>();

    List<Long> fileIds = new ArrayList<>();
    List<String> fileAttr = new ArrayList<>();
    Iterator<INode> iterator = inodes.iterator();
    while (iterator.hasNext()) {
      INode inode = iterator.next();
      if (inode == null) continue;
      strAttr.add(inode.getLocalName());
      if (inode.getId() == 16385) {
        strAttr.add(" ");
      } else {
        strAttr.add(inode.getParentName());
      }
      longAttr.add(inode.getParentId());
      longAttr.add(inode.getId());
      longAttr.add(inode.getModificationTime());
      longAttr.add(inode.getAccessTime());
      longAttr.add(inode.getPermissionLong());
      if (inode.isDirectory()) {
        longAttr.add(0L);
      } else {
        longAttr.add(inode.asFile().getHeaderLong());
        FileUnderConstructionFeature uc = inode.asFile().getFileUnderConstructionFeature();
        if (uc != null) {
          fileIds.add(inode.getId());
          fileAttr.add(uc.getClientName(inode.getId()));
          fileAttr.add(uc.getClientMachine(inode.getId()));
        }
      }
      iterator.remove();
    }
    try {
      if (strAttr.size() > 0) {
        DatabaseINode.batchUpdateINodes(longAttr, strAttr, fileIds, fileAttr);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private final void invalidateAndWriteBackDB(String path) {
    Queue<ImmutablePair<String, String>> q = new LinkedList<>();
    q.add(new ImmutablePair<>(getParentName(), getLocalName()));

    ImmutablePair<String, String> id = null;
    Set<INode> inodes = new HashSet<>();
    while ((id = q.poll()) != null) {
      INode child = FSDirectory.getInstance().getInode(id.getLeft(), id.getRight());   
      if (child != null) {
        if (child.isDirectory()) {
          HashSet<String> childNames = ((INodeDirectory)child).getCurrentChildrenList2();
          for (String cname : childNames) {
            q.add(new ImmutablePair<>(child.getPath(), cname));
          }
        }
        inodes.add(child);
        // invalidate inode
        INodeKeyedObjects.getCache().invalidate(child.getPath());
        if (inodes.size() >= 5120) {
          // write back to db
          update_subtree(inodes);
        }
      }
      if (inodes.size() > 0) {
        // write back to db
        update_subtree(inodes);
      }
    }

  }

  private final void remoteChmod(Set<Pair<String, String>> mpoints) {
    // 1. invalidate cache and write back dirty data
    invalidateAndWriteBackDB(getPath());
    // 2. execute distributed txn
    List<String> parents = new ArrayList<>();
    List<String> names = new ArrayList<>();
    for (Pair<String, String> pair : mpoints) {
      File file = new File(pair.getLeft());
      parents.add(file.getParent());
      names.add(file.getName());

      String url = pair.getRight();
    }
    DatabaseINode.setPermissions(parents, names, this.permission);
  }

  private final void updatePermissionStatus(PermissionStatusFormat f, long n) {
    permission = f.BITS.combine(n, getPermissionLong());
    if (FSDirectory.getInstance().isLocalNN()) {
      INodeKeyedObjects.getUpdateSet().add(getPath());
    } else if (isDirectory()) {
      try {
        Set<Pair<String, String>> mpoints = FSDirectory.getInstance().getMountsManager().resolveSubPaths(getPath());
        LOG.info(getPath() + " has sub-paths that are mounted into: " + mpoints);
        remoteChmod(mpoints);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
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
    if (permission == -1L) {
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
    INodeKeyedObjects.getUpdateSet().add(getPath());
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

  /** Set last access time of inode. */
  @Override
  public final void setAccessTime(long accessTime) {
    this.accessTime = accessTime;
    INodeKeyedObjects.getUpdateSet().add(getPath());
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
    CompletableFuture.runAsync(
        () -> {
          DatabaseINode.removeXAttr(id);
        },
        Database.getInstance().getExecutorService());
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
    throw new IllegalStateException("Feature " + f.getClass().getSimpleName() + " not found.");
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
    if (f1 != null) throw new IllegalStateException("Duplicated ACLFeature");

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
