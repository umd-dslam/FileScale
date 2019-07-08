import com.github.benmanes.caffeine.cache.*;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.*;

// Many keys to single value
// https://stackoverflow.com/questions/53084384/caffeine-cache-many-keys-to-single-value
public class IndexedCache<K, V> implements Cache<K, V> {

  private Cache<K, V> cache;
  private Map<Class<?>, Map<Object, Set<K>>> indexes;

  private IndexedCache(Builder<K, V> bldr) {
    this.indexes = bldr.indexes;
    cache = bldr.caf.build();
  }

  public <R> void invalidateAllWithIndex(Class<R> clazz, R value) {
    cache.invalidateAll(indexes.get(clazz).getOrDefault(value, new HashSet<>()));
  }

  @Override
  public long estimatedSize() {
    return cache.estimatedSize();
  }

  @Override
  public Policy<K, V> policy() {
    return cache.policy();
  }

  @Override
  public void invalidateAll() {
    cache.invalidateAll();
  }

  @Override
  public void invalidateAll(Iterable<?> keys) {
    cache.invalidateAll(keys);
  }

  @Override
  public V getIfPresent(Object key) {
    return cache.getIfPresent(key);
  }

  @Override
  public V get(K key, Function<? super K, ? extends V> mappingFunction) {
    return cache.get(key, mappingFunction);
  }

  @Override
  public Map<K, V> getAllPresent(Iterable<?> keys) {
    return cache.getAllPresent(keys);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> map) {
    cache.putAll(map);
  }

  @Override
  public void put(K key, V value) {
    cache.put(key, value);
  }

  @Override
  public void invalidate(Object key) {
    cache.invalidate(key);
  }

  @Override
  public CacheStats stats() {
    return cache.stats();
  }

  @Override
  public ConcurrentMap<K, V> asMap() {
    return cache.asMap();
  }

  @Override
  public void cleanUp() {
    cache.cleanUp();
  }

  public static class Builder<K, V> {
    Map<Class<?>, Function<K, ?>> functions = new HashMap<>();
    Map<Class<?>, Map<Object, Set<K>>> indexes = new ConcurrentHashMap<>();
    Caffeine<K, V> caf;

    public <R> Builder<K, V> withIndex(Class<R> clazz, Function<K, R> function) {
      functions.put(clazz, function);
      indexes.put(clazz, new ConcurrentHashMap<>());
      return this;
    }

    public IndexedCache<K, V> buildFromCaffeine(Caffeine<Object, Object> caffeine) {
      caf =
          caffeine.writer(
              new CacheWriter<K, V>() {

                @Override
                public void write(K k, V v) {
                  for (Map.Entry<Class<?>, Map<Object, Set<K>>> indexesEntry : indexes.entrySet()) {
                    indexesEntry
                        .getValue()
                        .computeIfAbsent(
                            functions.get(indexesEntry.getKey()).apply(k), (ky) -> new HashSet<>())
                        .add(k);
                  }
                }

                @Override
                public void delete(K k, V v, RemovalCause removalCause) {
                  for (Map.Entry<Class<?>, Map<Object, Set<K>>> indexesEntry : indexes.entrySet()) {
                    indexesEntry.getValue().remove(functions.get(indexesEntry.getKey()).apply(k));
                  }
                }
              });
      return new IndexedCache<>(this);
    }
  }
}
