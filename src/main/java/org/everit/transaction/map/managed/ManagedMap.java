/*
 * Copyright (C) 2011 Everit Kft. (http://www.everit.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.everit.transaction.map.managed;

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
 * The implementation calls the functions of a wrapped {@link TransactionalMap}.
 *
 * @param <K>
 *          the type of keys maintained by this map
 * @param <V>
 *          the type of mapped values
 */
public class ManagedMap<K, V> implements Map<K, V> {

  /**
   * XAResource to manage the map.
   */
  protected class MapXAResource implements XAResource {

    public static final int DEFAULT_TRANSACTION_TIMEOUT = Integer.MAX_VALUE;

    protected final Transaction transaction;

    public MapXAResource(final Transaction transaction) {
      this.transaction = transaction;
    }

    @Override
    public void commit(final Xid xid, final boolean onePhase) throws XAException {
      updateTransactionState(transaction);
      wrapped.commitTransaction();
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
      updateTransactionState(transaction);
      wrapped.rollbackTransaction();
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

  protected final Map<Transaction, Boolean> enlistedTransactions = new WeakHashMap<>();

  protected final TransactionManager transactionManager;

  protected final TransactionalMap<K, V> wrapped;

  protected final ReentrantReadWriteLock wrappedRWLock = new ReentrantReadWriteLock();

  /**
   * Constructor.
   *
   * @param transactionalMap
   *          The Map that should is managed by this class.
   * @param transactionManager
   *          The JTA transaction manager.
   * @throws NullPointerException
   *           if transactionalMap or transactionManager is null.
   */
  public ManagedMap(final TransactionalMap<K, V> transactionalMap,
      final TransactionManager transactionManager) {
    Objects.requireNonNull(transactionManager, "Transaction manager cannot be null");
    Objects.requireNonNull(transactionalMap, "Transactional map cannot be null");
    this.transactionManager = transactionManager;
    this.wrapped = transactionalMap;

  }

  @Override
  public void clear() {
    updateTransactionState();
    wrapped.clear();
  }

  @Override
  public boolean containsKey(final Object key) {
    updateTransactionState();
    return wrapped.containsKey(key);
  }

  @Override
  public boolean containsValue(final Object value) {
    updateTransactionState();
    return wrapped.containsValue(value);
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    updateTransactionState();
    return wrapped.entrySet();
  }

  @Override
  public V get(final Object key) {
    updateTransactionState();
    return wrapped.get(key);
  }

  @Override
  public boolean isEmpty() {
    updateTransactionState();
    return wrapped.isEmpty();
  }

  @Override
  public Set<K> keySet() {
    updateTransactionState();
    return wrapped.keySet();
  }

  @Override
  public V put(final K key, final V value) {
    updateTransactionState();
    return wrapped.put(key, value);
  }

  @Override
  public void putAll(final Map<? extends K, ? extends V> m) {
    updateTransactionState();
    wrapped.putAll(m);
  }

  @Override
  public V remove(final Object key) {
    updateTransactionState();
    return wrapped.remove(key);
  }

  @Override
  public int size() {
    updateTransactionState();
    return wrapped.size();
  }

  /**
   * Checks if the Map is associated to a transaction and manages its context based on the current
   * Transaction state.
   *
   * @return The transaction that is associated to the Map after return.
   */
  protected Transaction updateTransactionState() {
    try {
      int status = transactionManager.getStatus();
      if (status == Status.STATUS_NO_TRANSACTION) {
        updateTransactionState(null);
      } else {
        Transaction currentTransaction = transactionManager.getTransaction();
        updateTransactionState(currentTransaction);
        return currentTransaction;
      }
      return null;
    } catch (SystemException e) {
      throw new UncheckedSystemException(e);
    }
  }

  /**
   * Manages the state of the wrapped {@link TransactionalMap} based on the current transaction.
   *
   * @param currentTransaction
   *          The current transaction.
   */
  protected void updateTransactionState(final Transaction currentTransaction) {
    try {
      if (currentTransaction == null) {
        if (wrapped.getAssociatedTransaction() != null) {
          wrapped.suspendTransaction();
        }
      } else {
        Object transactionOfWrappedMap = wrapped.getAssociatedTransaction();
        if (transactionOfWrappedMap != null
            && !transactionOfWrappedMap.equals(currentTransaction)) {
          wrapped.suspendTransaction();
        }

        if (transactionOfWrappedMap == null
            || !transactionOfWrappedMap.equals(currentTransaction)) {

          Boolean enlisgedIfNotNull = enlistedTransactions.get(currentTransaction);
          if (enlisgedIfNotNull != null) {
            wrapped.resumeTransaction(currentTransaction);
          } else {
            try {
              currentTransaction.enlistResource(new MapXAResource(currentTransaction));
            } catch (RollbackException e) {
              throw new UncheckedRollbackException(e);
            }
            wrapped.startTransaction(currentTransaction);
            enlistedTransactions.put(currentTransaction, Boolean.TRUE);
          }
        }
      }
    } catch (SystemException e) {
      throw new UncheckedSystemException(e);
    }

  }

  @Override
  public Collection<V> values() {
    updateTransactionState();
    return wrapped.values();
  }

}
