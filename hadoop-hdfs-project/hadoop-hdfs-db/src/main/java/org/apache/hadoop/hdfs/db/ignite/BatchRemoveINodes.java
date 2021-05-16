package org.apache.hadoop.hdfs.db.ignite;

import java.util.List;
import java.util.HashMap;
import java.util.Set;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignite;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.SqlFieldsQuery;

public class BatchRemoveINodes implements IgniteClosure<Set<BinaryObject>, String> {

    @IgniteInstanceResource
    private Ignite ignite;

    @Override
    public String apply(Set<BinaryObject> keys) {
        IgniteCache<BinaryObject, BinaryObject> inodesBinary = ignite.cache("inodes").withKeepBinary();
        inodesBinary.removeAll(keys);

        FileWriteAheadLogManager walMgr = (FileWriteAheadLogManager)(
            ((IgniteEx)ignite).context().cache().context().wal());
        return walMgr.lastWritePointer().toString();
    }
}
