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
 * The implementation is based on {@link org.apache.commons.transaction.memory.BasicTxMap}.
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

    private static final int DEFAULT_TRANSACTION_TIMEOUT = Integer.MAX_VALUE;

    @Override
    public void commit(final Xid xid, final boolean onePhase) throws XAException {
      Transaction transaction = handleTransactionState();
      wrapped.commitTransaction();
      enlistedTransactions.remove(transaction);
    }

    @Override
    public void end(final Xid xid, final int flags) throws XAException {
      System.out.println("end called " + flags);
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
      enlistedTransactions.remove(transaction);
    }

    @Override
    public boolean setTransactionTimeout(final int seconds) {
      return false;
    }

    @Override
    public void start(final Xid xid, final int flags) throws XAException {
      System.out.println("Start called //////////////////" + flags);
    }

  }

  protected final Map<Transaction, Boolean> enlistedTransactions = new WeakHashMap<>();

  protected final XAResource mapXAResource = new MapXAResource();

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
        if (wrapped.getAssociatedTransaction() != null) {
          wrapped.suspendTransaction();
        }
      } else {
        Object transactionOfWrappedMap = wrapped.getAssociatedTransaction();
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
          } else {
            try {
              currentTransaction.enlistResource(mapXAResource);
            } catch (RollbackException e) {
              throw new UncheckedRollbackException(e);
            }
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
