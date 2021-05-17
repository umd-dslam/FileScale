package org.apache.hadoop.hdfs.db.ignite;

public class RenamePayload {
    public long dir_id;
    public long dest_id;
    public String old_parent_name;
    public String new_parent_name;
    public long new_parent;

    public RenamePayload(long dir_id, long dest_id, String old_parent_name, String new_parent_name, long new_parent) {
        this.dir_id = dir_id;
        this.dest_id = dest_id;
        this.old_parent_name = old_parent_name;
        this.new_parent_name = new_parent_name;
        this.new_parent = new_parent;
    }
}
