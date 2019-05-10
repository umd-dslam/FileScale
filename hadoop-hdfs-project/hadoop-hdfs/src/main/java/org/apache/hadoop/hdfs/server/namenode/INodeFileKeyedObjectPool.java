package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;

public class INodeFileKeyedObjectPool extends GenericKeyedObjectPool<Long, INodeFile> {

    public INodeFileKeyedObjectPool(INodeFileKeyedObjFactory factory) {
        super(factory);
    }

    public INodeFileKeyedObjectPool(INodeFileKeyedObjFactory factory, GenericKeyedObjectPoolConfig config) {
        super(factory, config);
    }

    public INodeDirectory getActiveObject(Long key) {
        final ObjectDeque<INodeFile> deque = poolMap.get(key);;
        // only one object exists in each sub-pool per key
        for (final PooledObject<INodeFile> p : deque.getAllObjects().values()) {
            return p.getObject();
        }
        return null;
    }
}
