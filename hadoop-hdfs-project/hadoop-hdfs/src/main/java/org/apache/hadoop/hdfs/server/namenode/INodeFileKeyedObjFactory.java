package org.apache.hadoop.hdfs.server.namenode;

import java.util.HashSet;
import java.util.Set;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;


public class INodeFileKeyedObjFactory extends BaseKeyedPooledObjectFactory<Long, INodeFile> {
  private Set<Long> allIds;

  public INodeFileKeyedObjFactory() {
    super();
    allIds = new HashSet<Long>();
  }

  public boolean find(Long id) {
    return allIds.contains(id);
  }

  @Override
  public INodeFile create(Long id) {
    allIds.add(id);
    return new INodeFile(id);
  }

  /** Use the default PooledObject implementation. */
  @Override
  public PooledObject<INodeFile> wrap(INodeFile inode) {
    return new DefaultPooledObject<INodeFile>(inode);
  }

  @Override
  public PooledObject<INodeFile> makeObject(Long id) throws Exception {
    return super.makeObject(id);
  }

  @Override
  public void activateObject(PooledObject<INodeFile> pooledObject) throws Exception {
    super.activateObject(pooledObject);
  }

  @Override
  public void destroyObject(Long id, PooledObject<INodeFile> pooledObject) {
    allIds.remove(id);
  }
}
