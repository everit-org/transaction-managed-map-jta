package org.everit.transaction.managed.map.jta;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.everit.transaction.managed.map.jta.internal.RWLockedMap;
import org.everit.transaction.unchecked.UncheckedRollbackException;
import org.everit.transaction.unchecked.UncheckedSystemException;

/**
 * Map that manages its transactional state by enlisting itself if there is an ongoing transaction.
 * The implementation is based on {@link org.apache.commons.transaction.memory.BasicTxMap}.
 *
 * @param <K>
 *          the type of keys maintained by this map
 * @param <V>
 *          the type of mapped values
 */
public class ManagedMap<K, V> implements Map<K, V> {

  /**
   * Mostly copied from org.apache.commons.collections.map.AbstractHashedMap.
   *
   * @param <K>
   *          The type of the key of the Map.
   * @param <V>
   *          The type of the value of the Map.
   */
  protected static class HashEntry<K, V> implements Entry<K, V> {

    protected K key;

    protected V value;

    protected HashEntry(final K key, final V value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof Map.Entry)) {
        return false;
      }
      @SuppressWarnings("unchecked")
      Map.Entry<K, V> other = (Map.Entry<K, V>) obj;
      return (getKey() == null ? other.getKey() == null : getKey().equals(other.getKey()))
          && (getValue() == null ? other.getValue() == null : getValue().equals(
              other.getValue()));
    }

    @Override
    public K getKey() {
      return key;
    }

    @Override
    public V getValue() {
      return value;
    }

    @Override
    public int hashCode() {
      return (getKey() == null ? 0 : getKey().hashCode())
          ^ (getValue() == null ? 0 : getValue().hashCode());
    }

    @Override
    public V setValue(final V value) {
      V old = this.value;
      this.value = value;
      return old;
    }

    @Override
    public String toString() {
      return new StringBuffer().append(getKey()).append('=').append(getValue()).toString();
    }
  }

  protected static class LockWithCount {

    public int count = 0;

    public Lock lock = new ReentrantLock();
  }

  /**
   * Stores the temporary changes of the Map that might be applied in the end of the transaciton.
   * Copied from 2-SNAPSHOT version of Apache Commons Transaction and modified.
   */
  protected class MapTxContext implements Map<K, V> {
    protected Map<K, V> adds;

    protected Map<K, V> changes;

    protected boolean cleared;

    protected Set<K> deletes;

    protected boolean readOnly = true;

    /**
     * Constructor.
     */
    public MapTxContext() {
      deletes = new HashSet<K>();
      changes = new HashMap<K, V>();
      adds = new HashMap<K, V>();
      cleared = false;
    }

    @Override
    public void clear() {
      readOnly = false;
      cleared = true;
      deletes.clear();
      changes.clear();
      adds.clear();
    }

    /**
     * Writes the temporary changes back to the Map.
     */
    public void commit() {
      WriteLock writeLock = wrappedRWLock.writeLock();
      writeLock.lock();
      try {
        if (!isReadOnly()) {

          if (cleared) {
            wrapped.clear();
          }

          wrapped.putAll(changes);
          wrapped.putAll(adds);

          for (Object key : deletes) {
            wrapped.remove(key);
          }
        }
      } finally {
        writeLock.unlock();
      }
    }

    @Override
    public boolean containsKey(final Object key) {
      if (deletes.contains(key)) {
        // reflects that entry has been deleted in this tx
        return false;
      }

      if (changes.containsKey(key)) {
        return true;
      }

      if (adds.containsKey(key)) {
        return true;
      }

      if (cleared) {
        return false;
      } else {
        // not modified in this tx
        return wrapped.containsKey(key);
      }
    }

    @Override
    public boolean containsValue(final Object value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
      Set<Entry<K, V>> entrySet = new HashSet<>();
      // XXX expensive :(
      for (K key : keySet()) {
        V value = get(key);
        // XXX we have no isolation, so get entry might have been
        // deleted in the meantime
        if (value != null) {
          entrySet.add(new HashEntry<K, V>(key, value));
        }
      }
      return entrySet;
    }

    @Override
    public V get(final Object key) {

      if (deletes.contains(key)) {
        // reflects that entry has been deleted in this tx
        return null;
      }

      if (changes.containsKey(key)) {
        return changes.get(key);
      }

      if (adds.containsKey(key)) {
        return adds.get(key);
      }

      if (cleared) {
        return null;
      } else {
        // not modified in this tx
        return wrapped.get(key);
      }
    }

    @Override
    public boolean isEmpty() {
      return (size() == 0);
    }

    public boolean isReadOnly() {
      return readOnly;
    }

    @Override
    public Set<K> keySet() {
      Set<K> keySet = new HashSet<K>();
      if (!cleared) {
        keySet.addAll(wrapped.keySet());
        keySet.removeAll(deletes);
      }
      keySet.addAll(adds.keySet());
      return keySet;
    }

    @Override
    public V put(final K key, final V value) {
      readOnly = false;

      V oldValue = get(key);

      deletes.remove(key);
      if (wrapped.containsKey(key)) {
        changes.put(key, value);
      } else {
        adds.put(key, value);
      }

      return oldValue;
    }

    @Override
    public void putAll(final Map<? extends K, ? extends V> map) {
      for (Object name : map.entrySet()) {
        @SuppressWarnings({ "rawtypes", "unchecked" })
        Map.Entry<K, V> entry = (Map.Entry) name;
        put(entry.getKey(), entry.getValue());
      }
    }

    @Override
    public V remove(final Object key) {
      V oldValue = get(key);

      readOnly = false;
      changes.remove(key);
      adds.remove(key);
      if (wrapped.containsKey(key) && !cleared) {
        @SuppressWarnings("unchecked")
        K typedKey = (K) key;
        deletes.add(typedKey);
      }

      return oldValue;
    }

    @Override
    public int size() {
      int size = (cleared ? 0 : wrapped.size());

      size -= deletes.size();
      size += adds.size();

      return size;
    }

    @Override
    public Collection<V> values() {
      throw new UnsupportedOperationException();
    }

  }

  /**
   * XAResource to manage the map.
   */
  protected class MapXAResource implements XAResource {

    private static final int DEFAULT_TRANSACTION_TIMEOUT = Integer.MAX_VALUE;

    @Override
    public void commit(final Xid xid, final boolean onePhase) throws XAException {
      Transaction transaction = handleTransactionState();
      getActiveTx().commit();
      setActiveTx(null);
      transactionOfWrappedMapTL.set(null);
      enlistedTransactions.remove(transaction);
    }

    @Override
    public void end(final Xid xid, final int flags) throws XAException {
      // Do nothing as only commit and fail are handled.
    }

    @Override
    public void forget(final Xid xid) {
    }

    protected ManagedMap<K, V> getManagedMap() {
      return ManagedMap.this;
    }

    @Override
    public int getTransactionTimeout() {
      return DEFAULT_TRANSACTION_TIMEOUT;
    }

    @Override
    public boolean isSameRM(final XAResource xares) {
      if (xares instanceof ManagedMap.MapXAResource) {
        @SuppressWarnings("rawtypes")
        ManagedMap.MapXAResource mapXares = (ManagedMap.MapXAResource) xares;
        return getManagedMap().equals(mapXares.getManagedMap());
      }
      return false;
    }

    @Override
    public int prepare(final Xid xid) throws XAException {
      return XAResource.XA_OK;
    }

    @Override
    public Xid[] recover(final int flag) throws XAException {
      return new Xid[0];
    }

    @Override
    public void rollback(final Xid xid) throws XAException {
      Transaction transaction = handleTransactionState();
      setActiveTx(null);
      transactionOfWrappedMapTL.set(null);
      enlistedTransactions.remove(transaction);
    }

    @Override
    public boolean setTransactionTimeout(final int seconds) {
      return false;
    }

    @Override
    public void start(final Xid xid, final int flags) throws XAException {
    }

  }

  protected ThreadLocal<MapTxContext> activeTx = new ThreadLocal<>();

  protected final Map<Transaction, Map<K, Lock>> enlistedTransactions =
      new WeakHashMap<>();

  protected Map<K, LockWithCount> lockByKey = new HashMap<>();

  protected final XAResource mapXAResource = new MapXAResource();

  protected final Map<Object, MapTxContext> suspendedTXContexts = new ConcurrentHashMap<>();

  protected final TransactionManager transactionManager;

  protected final ThreadLocal<Transaction> transactionOfWrappedMapTL = new ThreadLocal<>();

  protected final Map<K, V> wrapped;

  protected final ReentrantReadWriteLock wrappedRWLock = new ReentrantReadWriteLock();

  public ManagedMap(final TransactionManager transactionManager) {
    this(transactionManager, null);
  }

  /**
   * Constructor.
   *
   * @param transactionManager
   *          The JTA transaction manager.
   * @param wrapped
   *          The Map that should is managed by this class.
   */
  public ManagedMap(final TransactionManager transactionManager,
      final Map<K, V> wrapped) {
    this.transactionManager = transactionManager;
    if (wrapped != null) {
      this.wrapped = new RWLockedMap<>(wrapped, wrappedRWLock);
    } else {
      this.wrapped = new RWLockedMap<>(new HashMap<>(), wrappedRWLock);
    }
  }

  @Override
  public void clear() {
    handleTransactionState();
    coalesceActiveTxOrWrapped().clear();
  }

  protected Map<K, V> coalesceActiveTxOrWrapped() {
    ManagedMap<K, V>.MapTxContext txContext = getActiveTx();
    return (txContext != null) ? txContext : wrapped;
  }

  @Override
  public boolean containsKey(final Object key) {
    handleTransactionState();
    return coalesceActiveTxOrWrapped().containsKey(key);
  }

  @Override
  public boolean containsValue(final Object value) {
    handleTransactionState();
    return coalesceActiveTxOrWrapped().containsValue(value);
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    handleTransactionState();
    return coalesceActiveTxOrWrapped().entrySet();
  }

  @Override
  public V get(final Object key) {
    handleTransactionState();
    return coalesceActiveTxOrWrapped().get(key);
  }

  protected MapTxContext getActiveTx() {
    return activeTx.get();
  }

  protected synchronized Lock getLockInfoByKeyAndIncrementCount(final K key) {
    LockWithCount lockWithCount = lockByKey.get(key);
    if (lockWithCount == null) {
      if (!containsKey(key)) {
        return null;
      }
      lockWithCount = new LockWithCount();
      lockWithCount.count++;
      lockByKey.put(key, lockWithCount);
    } else {
      lockWithCount.count++;
    }
    return lockWithCount.lock;
  }

  /**
   * Checks if the Map is associated to a transaction and manages its context based on the current
   * Transaction state.
   *
   * @return The transaction that is associated to the Map after return.
   */
  protected Transaction handleTransactionState() {
    try {
      int status = transactionManager.getStatus();
      if (status == Status.STATUS_NO_TRANSACTION) {
        Transaction transactionOfWrappedMap = transactionOfWrappedMapTL.get();
        if (transactionOfWrappedMap != null) {
          suspendTransaction(transactionOfWrappedMap);
        }
      } else {
        Transaction transactionOfWrappedMap = transactionOfWrappedMapTL.get();
        Transaction currentTransaction = transactionManager.getTransaction();
        if (transactionOfWrappedMap != null
            && !transactionOfWrappedMap.equals(currentTransaction)) {
          suspendTransaction(transactionOfWrappedMap);
        }

        if (transactionOfWrappedMap == null
            || !transactionOfWrappedMap.equals(currentTransaction)) {

          Map<K, Lock> enlisgedIfNotNull = enlistedTransactions.get(currentTransaction);
          if (enlisgedIfNotNull != null) {
            resumeTransaction(currentTransaction);
            transactionOfWrappedMapTL.set(currentTransaction);
          } else {
            try {
              currentTransaction.enlistResource(mapXAResource);
            } catch (RollbackException e) {
              throw new UncheckedRollbackException(e);
            }
            transactionOfWrappedMapTL.set(currentTransaction);
            startTransaction();
            enlistedTransactions.put(currentTransaction, Collections.emptyMap());
          }
        }
        return currentTransaction;
      }
      return null;
    } catch (SystemException e) {
      throw new UncheckedSystemException(e);
    }

  }

  @Override
  public boolean isEmpty() {
    handleTransactionState();
    return coalesceActiveTxOrWrapped().isEmpty();
  }

  protected synchronized void kCountByKey(final K key) {

  }

  @Override
  public Set<K> keySet() {
    handleTransactionState();
    return coalesceActiveTxOrWrapped().keySet();
  }

  public boolean lockByKey(final K key) {
    Transaction transaction = handleTransactionState();
    if (transaction == null) {
      throw new IllegalStateException("There must be an active transaction to lock by key");
    }

    Lock lock = wrappedRWLock.writeLock();
    lock.lock();
    try {
      Lock keyLock = getLockInfoByKeyAndIncrementCount(key);

      // TODO

      return true;
    } finally {
      lock.unlock();
    }
  }

  protected Lock lockKey(final K key) {
    Lock lock = getLockInfoByKeyAndIncrementCount(key);
    if (lock == null) {
      return null;
    }

    lock.lock();
    if (!containsKey(key)) {

    }
  }

  @Override
  public V put(final K key, final V value) {
    handleTransactionState();
    return coalesceActiveTxOrWrapped().put(key, value);
  }

  @Override
  public void putAll(final Map<? extends K, ? extends V> m) {
    handleTransactionState();
    coalesceActiveTxOrWrapped().putAll(m);
  }

  @Override
  public V remove(final Object key) {
    handleTransactionState();
    return coalesceActiveTxOrWrapped().remove(key);
  }

  /**
   * Resumes a transaction that was previously suspended.
   *
   * @param currentTransaction
   *          The transaction to resume.
   * @throws NullPointerException
   *           if there is no suspended transaction registered.
   */
  protected void resumeTransaction(final Transaction currentTransaction) {
    MapTxContext txContext = suspendedTXContexts.remove(currentTransaction);
    Objects.requireNonNull(txContext);
    setActiveTx(txContext);
  }

  protected void setActiveTx(final MapTxContext mapContext) {
    activeTx.set(mapContext);
  }

  @Override
  public int size() {
    handleTransactionState();
    return coalesceActiveTxOrWrapped().size();
  }

  protected void startTransaction() {
    setActiveTx(new MapTxContext());
  }

  /**
   * Suspends the context for a specific transaction.
   *
   * @param transactionOfWrappedMap
   *          The transaction that is currently mapped to the Map context.
   */
  protected void suspendTransaction(final Transaction transactionOfWrappedMap) {
    MapTxContext activeTx = getActiveTx();
    Objects.requireNonNull(activeTx, "No active map context for transaction.");
    suspendedTXContexts.put(transactionOfWrappedMap, activeTx);
    setActiveTx(null);
  }

  @Override
  public Collection<V> values() {
    throw new UnsupportedOperationException();
  }

}
