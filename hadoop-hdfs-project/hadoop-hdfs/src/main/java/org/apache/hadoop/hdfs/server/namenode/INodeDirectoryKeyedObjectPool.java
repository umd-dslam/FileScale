package org.apache.hadoop.hdfs.server.namenode;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class INodeDirectoryKeyedObjectPool extends GenericKeyedObjectPool<Long, INodeDirectory> {
  static final Logger LOG = LoggerFactory.getLogger(INodeDirectoryKeyedObjectPool.class);
  private final INodeDirectoryKeyedObjFactory factory;

  public INodeDirectoryKeyedObjectPool(INodeDirectoryKeyedObjFactory factory) {
    super(factory);
    this.factory = factory;
  }

  public INodeDirectoryKeyedObjectPool(
      INodeDirectoryKeyedObjFactory factory, GenericKeyedObjectPoolConfig config) {
    super(factory, config);
    this.factory = factory;
  }

  public INodeDirectory getObject(Long key) {
    INodeDirectory obj = null;
    try {
      if (getNumActive(key) > 0) {
        LOG.info("get INodeDirectory Object (" + key + ") from Pool via borrowActiveObject");
        obj = borrowActiveObject(key);
      } else {
        LOG.info("get INodeDirectory Object (" + key + ") from Pool via borrowObject");
        obj = borrowObject(key);
      }
    } catch (Exception e) {
      System.err.println("Failed to borrow a INode object : " + e.getMessage());
      e.printStackTrace();
      System.exit(0);
    }
    return obj;
  }

  private INodeDirectory borrowActiveObject(Long key) {
    factory.increment(key);
    return super.getActiveObject(key);
  }

  public void returnToPool(Long id, INodeDirectory inode) {
    factory.decrement(id);
    if (factory.getCount(id) == 0) {
      this.returnObject(id, inode);
    }
  }

  public boolean isInDirectoryPool(Long key) {
    return super.findObject(key);
  }

  // Reflection via run-time type information (RTTI)
  private Object getSpecificFieldObject(String fieldName) {
    Class<?> cls = this.getClass().getSuperclass();
    Object obj = null;
    try {
      Field field = cls.getDeclaredField(fieldName);
      field.setAccessible(true);
      obj = field.get(this);
    } catch (NoSuchFieldException e) {
      e.printStackTrace();
    } catch (SecurityException e) {
      e.printStackTrace();
    } catch (IllegalArgumentException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
    return obj;
  }

  private Method getSpecificFieldMethod(String MethodName) {
    Class<?> cls = this.getClass().getSuperclass();
    Method method = null;
    try {
      method = cls.getDeclaredMethod(MethodName);
      method.setAccessible(true);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return method;
  }
}
