package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.tuple.Pair;

public class CompositeKey {
  Long k1;    // INode ID
  String k2;  
  Pair<Long, String> k3;  // <Parent ID, INode Name>

  CompositeKey(Long k1, String k2, Pair<Long, String> k3) {
    this.k1 = k1;
    this.k2 = k2;
    this.k3 = k3;
  }

  CompositeKey(Long k1, Pair<Long, String> k3) {
    this.k1 = k1;
    this.k3 = k3;
  }

  Pair<Long, String> getK3() {
    return this.k3;
  }

  String getK2() {
    return this.k2;
  }

  Long getK1() {
    return this.k1;
  }

  @Override
  public boolean equals(Object o) {
    if ((o == null) || (o.getClass() != this.getClass())) {
      return false;
    }
    CompositeKey other = (CompositeKey) o;
    return new EqualsBuilder()
        .append(k1, other.k1)
        .append(k2, other.k2)
        .append(k3, other.k3)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(this.k1).append(this.k2).append(this.k3).toHashCode();
  }
}
