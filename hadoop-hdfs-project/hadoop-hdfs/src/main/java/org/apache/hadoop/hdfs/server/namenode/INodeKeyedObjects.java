package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class INodeKeyedObjects {
  private static INodeKeyedObjects instance;
  private INodeFileKeyedObjectPool filePool;
  private INodeDirectoryKeyedObjectPool directoryPool;

  INodeKeyedObjects() {
    try {
      initializePool();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(0);
    }
  }

  public static INodeKeyedObjects getInstance() {
    if (instance == null) {
      instance = new INodeKeyedObjects();
    }
    return instance;
  }

  public void returnToFilePool(long id, INodeFile inode) {
    filePool.getFactory().decrement(id);
    if (filePool.getFactory().getCount(id) == 0) {
      filePool.returnObject(id, inode);
    }
  }

  public boolean isInFilePool(long id) {
    return filePool.isInFilePool(id);
  }

  public void clearFile(long id) {
    filePool.clear(id);
  }

  public void returnToDirectoryPool(long id, INodeDirectory inode) {
    directoryPool.getFactory().decrement(id);
    if (directoryPool.getFactory().getCount(id) == 0) {
      directoryPool.returnObject(id, inode);
    }
  }

  public boolean isInDirectoryPool(long id) {
    return directoryPool.isInDirectoryPool(id);
  }

  public void clearDirectory(long id) {
    directoryPool.clear(id);
  }

  public INodeDirectory getINodeDirectory(Long id) {
    INodeDirectory obj = null;
    try {
      if (directoryPool.getNumActive(id) > 0) {
        obj = directoryPool.getActiveObject(id);
      } else {
        obj = directoryPool.borrowObject(id);
      }
    } catch (Exception e) {
      System.err.println("Failed to borrow a INode object : " + e.getMessage());
      e.printStackTrace();
      System.exit(0);
    }
    return obj;
  }

  public INodeFile getINodeFile(Long id) {
    INodeFile obj = null;
    try {
      if (filePool.getNumActive(id) > 0) {
        obj = filePool.getActiveObject(id);
      } else {
        obj = filePool.borrowObject(id);
      }
    } catch (Exception e) {
      System.err.println("Failed to borrow a INode object : " + e.getMessage());
      e.printStackTrace();
      System.exit(0);
    }
    return obj;
  }

  // A helper method to initialize the pool using the config and object-factory.
  private void initializePool() throws Exception {
    try {
      // https://commons.apache.org/proper/commons-pool/api-2.0/org/apache/commons/pool2/impl/DefaultEvictionPolicy.html

      filePool = new INodeFileKeyedObjectPool(new INodeFileKeyedObjFactory());
      String size = System.getenv("MAX_FILEPOOL_SIZE");
      if (size == null) {
        filePool.setMaxTotal(10000);
      } else {
        filePool.setMaxTotal(Integer.parseInt(size));
      }
      filePool.setMaxIdlePerKey(1);
      filePool.setMaxTotalPerKey(1);
    
      directoryPool = new INodeDirectoryKeyedObjectPool(new INodeDirectoryKeyedObjFactory());
      size = System.getenv("MAX_DIRECTORYPOOL_SIZE");
      if (size == null) {
        directoryPool.setMaxTotal(50000);
      } else {
        directoryPool.setMaxTotal(Integer.parseInt(size));
      }
      directoryPool.setMaxIdlePerKey(1);
      directoryPool.setMaxTotalPerKey(1);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(0);
    }
  }
}
