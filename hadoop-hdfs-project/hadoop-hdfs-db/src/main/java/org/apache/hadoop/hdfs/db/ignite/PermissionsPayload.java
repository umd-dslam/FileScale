package org.apache.hadoop.hdfs.db.ignite;

import java.util.Set;
import org.apache.ignite.binary.BinaryObject;

public class PermissionsPayload {
    public Set<BinaryObject> keys;
    public long permission;

    public PermissionsPayload(Set<BinaryObject> keys, long permission) {
        this.keys = keys;
        this.permission = permission;
    }
}
