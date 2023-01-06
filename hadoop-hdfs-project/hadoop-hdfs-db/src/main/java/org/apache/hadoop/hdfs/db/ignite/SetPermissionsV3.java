package org.apache.hadoop.hdfs.db.ignite;

import java.io.File;
import java.util.List;
import java.util.TreeSet;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

public class SetPermissionsV3 implements IgniteClosure<PermissionsPayload, String> {

    @IgniteInstanceResource
    private Ignite ignite;

    @Override
    public String apply(PermissionsPayload payload) {
        IgniteCache<BinaryObject, BinaryObject> inodesBinary = ignite.cache("inodes").withKeepBinary();

        File file = new File(payload.path);
        String parent = file.getParent();
        String name = file.getName();

        Transaction tx = ignite.transactions().txStart(
            TransactionConcurrency.PESSIMISTIC, TransactionIsolation.SERIALIZABLE);

        // 1. query subtree inodes
        ScanQuery<BinaryObject, BinaryObject> scanAddress = new ScanQuery<>(
            new IgniteBiPredicate<BinaryObject, BinaryObject>() {
                @Override
                public boolean apply(BinaryObject binaryKey, BinaryObject binaryObject) {
                    return ((String)binaryKey.field("parentName")).startsWith(parent);
                }
            }
        );
        Iterator<Cache.Entry<BinaryObject, BinaryObject>> iterator = inodesBinary.
            query(scanAddress).iterator();

        // 2. update subtree permission
        while (iterator.hasNext()) {
            BinaryObject key = iterator.next().getKey();
            BinaryObject value = inodesBinary.get(key);
            value = value.toBuilder()
                .setField("permission", payload.permission)
                .build();
            inodesBinary.put(key, value);
        }

        BinaryObjectBuilder inodeKeyBuilder = ignite.binary().builder("InodeKey");
        BinaryObject rootKey = inodeKeyBuilder
            .setField("parentName", parent)
            .setField("name", name)
            .build();
        BinaryObject rootValue = inodesBinary.get(rootKey);
        rootValue = rootValue.toBuilder()
            .setField("permission", payload.permission)
            .build();
        inodesBinary.put(rootKey, rootValue);

        tx.commit();
        tx.close();

        FileWriteAheadLogManager walMgr = (FileWriteAheadLogManager)(
            ((IgniteEx)ignite).context().cache().context().wal());
        return walMgr.lastWritePointer().toString();
    }
}
