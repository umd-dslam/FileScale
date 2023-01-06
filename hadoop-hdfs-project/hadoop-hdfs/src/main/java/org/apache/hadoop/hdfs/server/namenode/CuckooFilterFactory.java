package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import org.apache.hadoop.hdfs.cuckoofilter4j.*;
import org.apache.hadoop.hdfs.cuckoofilter4j.Utils.Algorithm;
import com.google.common.hash.Funnels;
import java.nio.charset.Charset;

public class CuckooFilterFactory extends BasePooledObjectFactory<CuckooFilter<CharSequence>> {
  public CuckooFilterFactory() {
    super();
  }

  @Override
  public CuckooFilter<CharSequence> create() throws Exception {
    int childNums = 1024;
    String nums = System.getenv("FILESCALE_FILES_PER_DIRECTORY");
    if (nums != null) {
      childNums = Integer.parseInt(nums);
    }
    return new CuckooFilter.Builder<CharSequence>(Funnels.stringFunnel(Charset.defaultCharset()), childNums)
      .withFalsePositiveRate(0.001).withHashAlgorithm(Algorithm.xxHash64).build();
  }

  /** Use the default PooledObject implementation. */
  @Override
  public PooledObject<CuckooFilter<CharSequence>> wrap(CuckooFilter<CharSequence> filter) {
    return new DefaultPooledObject<CuckooFilter<CharSequence>>(filter);
  }

  @Override
  public PooledObject<CuckooFilter<CharSequence>> makeObject() throws Exception {
    return super.makeObject();
  }

  @Override
  public void activateObject(PooledObject<CuckooFilter<CharSequence>> pooledObject) throws Exception {
    super.activateObject(pooledObject);
  }

  @Override
  public boolean validateObject(PooledObject<CuckooFilter<CharSequence>> pooledObject) {
    return true;
  }

  @Override
  public void destroyObject(PooledObject<CuckooFilter<CharSequence>> pooledObject) throws Exception {
    super.destroyObject(pooledObject);
  }
}
