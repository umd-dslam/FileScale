package org.apache.hadoop.hdfs.server.namenode;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class INodeKeyedObjects {
  static class CompositeKey {
    Long k1;
    String k2;
    Pair<Long, String> k3;

    CompositeKey(Long k1, String k2, Pair<Long, String> k3) {
      this.k1 = k1;
      this.k2 = k2;
      this.k3 = k3;
    }

    Pair<Long, String> getK3() {
      return this.k3;
    }

    String getK2() {
      return this.k2;
    }

    Long getK1() {
      return this.k1;
    }

    @Override
    public boolean equals(Object o) {
      if ((o == null) || (o.getClass() != this.getClass())) {
        return false;
      }
      CompositeKey other = (CompositeKey) o;
      return new EqualsBuilder()
          .append(k1, other.k1)
          .append(k2, other.k2)
          .append(k3, other.k3)
          .isEquals();
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder().append(this.k1).append(this.k2).append(this.k3).toHashCode();
    }
  }

  private static INodeKeyedObjects instance;

  static boolean use_cache = true;
  private static IndexedCache<CompositeKey, INode> cache;

  private INodeFileKeyedObjectPool filePool;
  private INodeDirectoryKeyedObjectPool directoryPool;

  static final Logger LOG = LoggerFactory.getLogger(INodeKeyedObjects.class);

  INodeKeyedObjects() {
    if (!use_cache) {
      try {
        initializePool();
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(0);
      }
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

  // --------------------------------------------------------
  // caffeine cache

  public static Cache<Long, INode> getCache() {
    if (cache == null) {
      // https://github.com/ben-manes/caffeine/wiki/Removal

      Caffeine<Object, Object> cfein =
          Caffeine.newBuilder()
              .removalListener(
                  (CompositeKey keys, INode inode, RemovalCause cause) -> {
                    if (cause == RemovalCause.EXPLICIT
                        || cause == RemovalCause.COLLECTED
                        || cause == RemovalCause.EXPIRED
                        || cause == RemovalCause.SIZE) {
                      LOG.info("Cache Evicted: INode=%s", keys.getK1());
                      // stored procedure: update inode in db
                      if (inode.isDirectory()) {
                        inode.asDirectory().updateINodeDirectory();
                      } else {
                        inode.asFile().updateINodeFile();
                      }
                    }
                  })
              .maximumSize(10_000);
      cache =
          new IndexedCache.Builder<CompositeKey, String>()
              .withIndex(Long.class, ck -> ck.getK1())
              .withIndex(String.class, ck -> ck.getK2())
              .withIndex(Pair.class, ck -> ck.getK3())
              .buildFromCaffeine(cfein);
    }
    return cache;
  }
}
