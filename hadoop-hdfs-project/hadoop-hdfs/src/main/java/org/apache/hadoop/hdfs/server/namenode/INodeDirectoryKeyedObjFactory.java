package org.apache.hadoop.hdfs.server.namenode;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;


public class INodeDirectoryKeyedObjFactory extends BaseKeyedPooledObjectFactory<Long, INodeFile> {

  private final ConcurrentHashMap<Long, AtomicInteger> map;

  public INodeDirectoryKeyedObjFactory() {
    super();
    map = new ConcurrentHashMap<>();
  }

  public void decrement(final Long id) {
    AtomicInteger value = map.get(id);
    if (value != null) {
      if (value.get() == 0) {
        map.remove(id);
      } else {
        value.decrementAndGet();
      }
    }
  }

  public void increment(final Long id) {
    // https://www.slideshare.net/sjlee0/robust-and-scalable-concurrent-programming-lesson-from-the-trenches
    // Page 33
    AtomicInteger value = map.get(id);
    if (value == null) {
      value = new AtomicInteger(0);
      AtomicInteger old = map.putIfAbsent(id, value);
      if (old != null) { value = old; }
    }
    value.incrementAndGet(); // increment the value atomically
  }

  public int getCount(final Long id) {
    AtomicInteger value = map.get(id);
    return (value == null) ? 0 : value.get();
  }

  @Override
  public INodeDirectory create(Long id) {
    increment(id);
    return new INodeDirectory(id);
  }

  /** Use the default PooledObject implementation. */
  @Override
  public PooledObject<INodeDirectory> wrap(INodeDirectory inode) {
    return new DefaultPooledObject<INodeDirectory>(inode);
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
    super.destroyObject(id, pooledObject);
    map.remove(id);
  }
}
