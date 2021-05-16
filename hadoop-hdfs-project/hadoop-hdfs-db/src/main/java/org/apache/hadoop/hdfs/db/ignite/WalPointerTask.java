package org.apache.hadoop.hdfs.db.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.resources.IgniteInstanceResource;

public class WalPointerTask implements IgniteCallable<String> {

    @IgniteInstanceResource
    private Ignite ignite;

    @Override
    public String call() throws Exception {
        FileWriteAheadLogManager walMgr = (FileWriteAheadLogManager)(
            ((IgniteEx)ignite).context().cache().context().wal());
        return walMgr.lastWritePointer().toString();
    }
}
