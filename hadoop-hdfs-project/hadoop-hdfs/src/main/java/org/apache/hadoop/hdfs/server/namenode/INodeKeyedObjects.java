package org.apache.hadoop.hdfs.server.namenode;

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

  public void returnToFilePool(final Long id, final INodeFile inode) {
    filePool.returnToPool(id, inode);
  }

  public void returnToDirectoryPool(final Long id, final INodeDirectory inode) {
    directoryPool.returnToPool(id, inode);
  }

  public boolean isInFilePool(final Long id) {
    return filePool.isInFilePool(id);
  }

  public boolean isInDirectoryPool(final Long id) {
    return directoryPool.isInDirectoryPool(id);
  }

  public void clearFile(final Long id) {
    filePool.clear(id);
  }

  public void clearDirectory(final Long id) {
    directoryPool.clear(id);
  }

  public INodeFile getINodeFile(final Long id) {
    return filePool.getObject(id);
  }

  public INodeDirectory getINodeDirectory(final Long id) {
    return directoryPool.getObject(id);
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
