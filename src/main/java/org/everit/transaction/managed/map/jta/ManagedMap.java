package org.everit.transaction.managed.map.jta;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.everit.transaction.map.TransactionalMap;
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

  /**
   * XAResource to manage the map.
   */
  protected class MapXAResource implements XAResource {

    private static final int DEFAULT_TRANSACTION_TIMEOUT = Integer.MAX_VALUE;

    @Override
    public void commit(final Xid xid, final boolean onePhase) throws XAException {
      Transaction transaction = handleTransactionState();
      wrapped.commitTransaction();
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
      wrapped.rollbackTransaction();
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

  protected final Map<Transaction, Boolean> enlistedTransactions =
      new WeakHashMap<>();

  protected final XAResource mapXAResource = new MapXAResource();

  protected final TransactionManager transactionManager;

  protected final ThreadLocal<Transaction> transactionOfWrappedMapTL = new ThreadLocal<>();

  protected final TransactionalMap<K, V> wrapped;

  protected final ReentrantReadWriteLock wrappedRWLock = new ReentrantReadWriteLock();

  /**
   * Constructor.
   *
   * @param transactionalMap
   *          The Map that should is managed by this class.
   * @param transactionManager
   *          The JTA transaction manager.
   */
  public ManagedMap(final TransactionalMap<K, V> transactionalMap,
      final TransactionManager transactionManager) {
    Objects.requireNonNull(transactionManager);
    Objects.requireNonNull(transactionalMap);
    this.transactionManager = transactionManager;
    this.wrapped = transactionalMap;

  }

  @Override
  public void clear() {
    handleTransactionState();
    wrapped.clear();
  }

  @Override
  public boolean containsKey(final Object key) {
    handleTransactionState();
    return wrapped.containsKey(key);
  }

  @Override
  public boolean containsValue(final Object value) {
    handleTransactionState();
    return wrapped.containsValue(value);
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    handleTransactionState();
    return wrapped.entrySet();
  }

  @Override
  public V get(final Object key) {
    handleTransactionState();
    return wrapped.get(key);
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
          wrapped.suspendTransaction();
        }
      } else {
        Transaction transactionOfWrappedMap = transactionOfWrappedMapTL.get();
        Transaction currentTransaction = transactionManager.getTransaction();
        if (transactionOfWrappedMap != null
            && !transactionOfWrappedMap.equals(currentTransaction)) {
          wrapped.suspendTransaction();
        }

        if (transactionOfWrappedMap == null
            || !transactionOfWrappedMap.equals(currentTransaction)) {

          Boolean enlisgedIfNotNull = enlistedTransactions.get(currentTransaction);
          if (enlisgedIfNotNull != null) {
            wrapped.resumeTransaction(currentTransaction);
            transactionOfWrappedMapTL.set(currentTransaction);
          } else {
            try {
              currentTransaction.enlistResource(mapXAResource);
            } catch (RollbackException e) {
              throw new UncheckedRollbackException(e);
            }
            transactionOfWrappedMapTL.set(currentTransaction);
            wrapped.startTransaction(currentTransaction);
            enlistedTransactions.put(currentTransaction, Boolean.TRUE);
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
    return wrapped.isEmpty();
  }

  protected synchronized void kCountByKey(final K key) {

  }

  @Override
  public Set<K> keySet() {
    handleTransactionState();
    return wrapped.keySet();
  }

  @Override
  public V put(final K key, final V value) {
    handleTransactionState();
    return wrapped.put(key, value);
  }

  @Override
  public void putAll(final Map<? extends K, ? extends V> m) {
    handleTransactionState();
    wrapped.putAll(m);
  }

  @Override
  public V remove(final Object key) {
    handleTransactionState();
    return wrapped.remove(key);
  }

  @Override
  public int size() {
    handleTransactionState();
    return wrapped.size();
  }

  @Override
  public Collection<V> values() {
    handleTransactionState();
    return wrapped.values();
  }

}
