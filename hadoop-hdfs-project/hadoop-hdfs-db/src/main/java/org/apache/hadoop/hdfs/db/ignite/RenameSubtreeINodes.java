package org.apache.hadoop.hdfs.db.ignite;

import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
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

// Ignite does not allow updating a primary key because the latter defines a partition the key
// and its value belong to statically. While the partition with all its data can change several
// cluster owners, the key always belongs to a single partition. The partition is calculated
// using a hash function applied to the key’s value.
// Thus, if a key needs to be updated it has to be removed and then inserted.
public class RenameSubtreeINodes implements IgniteClosure<RenamePayload, String> {

    @IgniteInstanceResource
    private Ignite ignite;

    @Override
    public String apply(RenamePayload payload) {
        IgniteCache<BinaryObject, BinaryObject> inodesBinary = ignite.cache("inodes").withKeepBinary();
        
        Transaction tx = ignite.transactions().txStart(
            TransactionConcurrency.PESSIMISTIC, TransactionIsolation.SERIALIZABLE);
        
        // 1. query subtree inodes
        List<Cache.Entry<BinaryObject, BinaryObject>> result;
        ScanQuery<BinaryObject, BinaryObject> scanAddress = new ScanQuery<>(
            new IgniteBiPredicate<BinaryObject, BinaryObject>() {
                @Override
                public boolean apply(BinaryObject binaryKey, BinaryObject binaryObject) {
                    return ((String)binaryKey.field("parentName")).startsWith(payload.old_parent_name);
                }
            }
        );
        result = inodesBinary.query(scanAddress).getAll();

        // 2. update subtree inodes
        Set<BinaryObject> keys = new HashSet<>();
        Map<BinaryObject, BinaryObject> map = new HashMap<>();
        BinaryObjectBuilder inodeKeyBuilder = ignite.binary().builder("InodeKey");
        for (Cache.Entry<BinaryObject, BinaryObject> entry : result) {
            BinaryObject inodeValue = entry.getValue();
            long id = inodeValue.field("id");
            if (payload.dir_id == id) {
                inodeValue = inodeValue.toBuilder()
                    .setField("parentName", payload.new_parent_name)
                    .setField("parent", payload.new_parent)
                    .setField("id", (long)inodeValue.field("id") + payload.dest_id)
                    .build();
            } else {
                inodeValue = inodeValue.toBuilder()
                    .setField("parentName", payload.new_parent_name
                        + ((String)inodeValue.field("parentName")).substring(payload.old_parent_name.length()))
                    .setField("parent", (long)inodeValue.field("parent") + payload.dest_id)
                    .setField("id", (long)inodeValue.field("id") + payload.dest_id)
                    .build();
            }

            BinaryObject inodeNewKey = inodeKeyBuilder
                .setField("parentName", (String)inodeValue.field("parentName"))
                .setField("name", (String)inodeValue.field("name"))
                .build();
            keys.add(entry.getKey());
            map.put(inodeNewKey, inodeValue);
        }
        // 3. write new inodes to DB
        inodesBinary.removeAll(keys);
        inodesBinary.putAll(map);

        tx.commit();
        tx.close();

        // return WAL pointer
        FileWriteAheadLogManager walMgr = (FileWriteAheadLogManager)(
            ((IgniteEx)ignite).context().cache().context().wal());
        return walMgr.lastWritePointer().toString();
    }
}
