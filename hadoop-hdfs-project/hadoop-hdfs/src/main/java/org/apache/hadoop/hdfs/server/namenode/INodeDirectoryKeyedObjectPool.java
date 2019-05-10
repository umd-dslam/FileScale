package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;

public class INodeDirectoryKeyedObjectPool extends GenericKeyedObjectPool<Long, INodeDirectory> {

    public INodeDirectoryKeyedObjectPool(INodeDirectoryKeyedObjFactory factory) {
        super(factory);
    }

    public INodeDirectoryKeyedObjectPool(INodeDirectoryKeyedObjFactory factory, GenericKeyedObjectPoolConfig config) {
        super(factory, config);
    }

    public INodeDirectory getActiveObject(Long key) {
        getFactory().increment(key);
        final ObjectDeque<INodeDirectory> deque = poolMap.get(key);;
        // only one object exists in each sub-ppol per key
        for (final PooledObject<INodeDirectory> p : deque.getAllObjects().values()) {
            return p.getObject();
        }
        return null;
    }

    public boolean isInDirectoryPool(Long key) {
        if (poolMap.get(key) != null) {
            return true;
        }
        return false;
    }
}
