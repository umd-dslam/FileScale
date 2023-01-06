package org.apache.hadoop.hdfs.db.ignite;

import java.util.Set;
import org.apache.ignite.binary.BinaryObject;

public class PermissionsPayload {
    public String path;
    public long permission;

    public PermissionsPayload(String path, long permission) {
        this.path = path;
        this.permission = permission;
    }
}
