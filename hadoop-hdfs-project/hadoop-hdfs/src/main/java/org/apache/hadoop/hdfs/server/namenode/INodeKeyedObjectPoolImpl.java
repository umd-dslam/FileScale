package org.apache.hadoop.hdfs.server.namenode;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.pool2.PooledObject;

interface INodeKeyedObjectPoolImpl {
  // Copy from common pool2

  /**
   * Wrapper for objects under management by the pool.
   *
   * <p>GenericObjectPool and GenericKeyedObjectPool maintain references to all objects under
   * management using maps keyed on the objects. This wrapper class ensures that objects can work as
   * hash keys.
   *
   * @param <T> type of objects in the pool
   */
  static class IdentityWrapper<T> {
    /** Wrapped object */
    private final T instance;

    /**
     * Create a wrapper for an instance.
     *
     * @param instance object to wrap
     */
    public IdentityWrapper(final T instance) {
      this.instance = instance;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(instance);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public boolean equals(final Object other) {
      return other instanceof IdentityWrapper && ((IdentityWrapper) other).instance == instance;
    }

    /** @return the wrapped object */
    public T getObject() {
      return instance;
    }

    @Override
    public String toString() {
      final StringBuilder builder = new StringBuilder();
      builder.append("IdentityWrapper [instance=");
      builder.append(instance);
      builder.append("]");
      return builder.toString();
    }
  }

  /**
   * Maintains information on the per key queue for a given key.
   *
   * @param <S> type of objects in the pool
   */
  class ObjectDeque<S> {

    private final LinkedBlockingDeque<PooledObject<S>> idleObjects;

    /*
     * Number of instances created - number destroyed.
     * Invariant: createCount <= maxTotalPerKey
     */
    private final AtomicInteger createCount = new AtomicInteger(0);

    private long makeObjectCount = 0;
    private final Object makeObjectCountLock = new Object();

    /*
     * The map is keyed on pooled instances, wrapped to ensure that
     * they work properly as keys.
     */
    private final Map<IdentityWrapper<S>, PooledObject<S>> allObjects = new ConcurrentHashMap<>();

    /*
     * Number of threads with registered interest in this key.
     * register(K) increments this counter and deRegister(K) decrements it.
     * Invariant: empty keyed pool will not be dropped unless numInterested
     *            is 0.
     */
    private final AtomicLong numInterested = new AtomicLong(0);

    /**
     * Create a new ObjecDeque with the given fairness policy.
     *
     * @param fairness true means client threads waiting to borrow / return instances will be served
     *     as if waiting in a FIFO queue.
     */
    public ObjectDeque(final boolean fairness) {
      idleObjects = new LinkedBlockingDeque<>(fairness);
    }

    /**
     * Obtain the idle objects for the current key.
     *
     * @return The idle objects
     */
    public LinkedBlockingDeque<PooledObject<S>> getIdleObjects() {
      return idleObjects;
    }

    /**
     * Obtain the count of the number of objects created for the current key.
     *
     * @return The number of objects created for this key
     */
    public AtomicInteger getCreateCount() {
      return createCount;
    }

    /**
     * Obtain the number of threads with an interest registered in this key.
     *
     * @return The number of threads with a registered interest in this key
     */
    public AtomicLong getNumInterested() {
      return numInterested;
    }

    /**
     * Obtain all the objects for the current key.
     *
     * @return All the objects
     */
    public Map<IdentityWrapper<S>, PooledObject<S>> getAllObjects() {
      return allObjects;
    }

    @Override
    public String toString() {
      final StringBuilder builder = new StringBuilder();
      builder.append("ObjectDeque [idleObjects=");
      builder.append(idleObjects);
      builder.append(", createCount=");
      builder.append(createCount);
      builder.append(", allObjects=");
      builder.append(allObjects);
      builder.append(", numInterested=");
      builder.append(numInterested);
      builder.append("]");
      return builder.toString();
    }
  }
}
