package org.everit.transaction.managed.map.jta.internal;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Supplier;

/**
 * A {@link Map} implementation that uses {@link ReadWriteLock} in its functions. Beware that this
 * class does not override the functions introduced in Java 8 as this class was designed to be used
 * internally within Managed Map project.
 *
 * @param <K>
 *          The type of the keys.
 * @param <V>
 *          The type of the values.
 */
public class RWLockedMap<K, V> implements Map<K, V> {

  protected final ReadWriteLock rwLock;

  protected final Map<K, V> wrapped;

  public RWLockedMap(final Map<K, V> wrapped, final ReadWriteLock readWriteLock) {
    this.wrapped = wrapped;
    this.rwLock = readWriteLock;
  }

  @Override
  public void clear() {
    doInLock(rwLock.writeLock(), () -> wrapped.clear());
  }

  @Override
  public boolean containsKey(final Object key) {
    return doInLock(rwLock.readLock(), (Supplier<Boolean>) () -> wrapped.containsKey(key));
  }

  @Override
  public boolean containsValue(final Object value) {
    return doInLock(rwLock.readLock(), (Supplier<Boolean>) () -> wrapped.containsValue(value));
  }

  /**
   * Does a specific action within the scope of a lock.
   *
   * @param lock
   *          The locker object.
   * @param action
   *          The action that should run.
   */
  protected void doInLock(final Lock lock, final Runnable action) {
    lock.lock();
    try {
      action.run();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Does a specific action within the scope of a lock.
   *
   * @param lock
   *          The locker object.
   * @param action
   *          The action that should run.
   */
  protected <R> R doInLock(final Lock lock, final Supplier<R> action) {
    lock.lock();
    try {
      return action.get();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return doInLock(rwLock.readLock(), () -> wrapped.entrySet());
  }

  @Override
  public V get(final Object key) {
    return doInLock(rwLock.readLock(), () -> wrapped.get(key));
  }

  @Override
  public boolean isEmpty() {
    return doInLock(rwLock.readLock(), (Supplier<Boolean>) () -> wrapped.isEmpty());
  }

  @Override
  public Set<K> keySet() {
    return doInLock(rwLock.readLock(), () -> wrapped.keySet());
  }

  @Override
  public V put(final K key, final V value) {
    return doInLock(rwLock.writeLock(), () -> wrapped.put(key, value));
  }

  @Override
  public void putAll(final Map<? extends K, ? extends V> m) {
    doInLock(rwLock.writeLock(), () -> wrapped.putAll(m));

  }

  @Override
  public V remove(final Object key) {
    return doInLock(rwLock.writeLock(), () -> wrapped.remove(key));
  }

  @Override
  public int size() {
    return doInLock(rwLock.readLock(), (Supplier<Integer>) () -> wrapped.size());
  }

  @Override
  public Collection<V> values() {
    return doInLock(rwLock.readLock(), () -> wrapped.values());
  }

}
