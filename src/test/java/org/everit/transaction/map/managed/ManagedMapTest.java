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

import java.util.Map;

import javax.naming.NamingException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;

import org.apache.geronimo.transaction.manager.GeronimoTransactionManager;
import org.everit.osgi.transaction.helper.api.TransactionHelper;
import org.everit.osgi.transaction.helper.internal.TransactionHelperImpl;
import org.everit.transaction.map.readcommited.ReadCommitedTransactionalMap;
import org.everit.transaction.unchecked.UncheckedSystemException;
import org.everit.transaction.unchecked.xa.UncheckedXAException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.objectweb.jotm.Jotm;

@RunWith(Theories.class)
public class ManagedMapTest {

  public static class TransactionManagerWithHelper {

    public final TransactionHelper transactionHelper;

    public final TransactionManager transactionManager;

    public TransactionManagerWithHelper(final TransactionManager transactionManager) {
      this.transactionManager = transactionManager;
      try {
        transactionManager.setTransactionTimeout(6000);
      } catch (SystemException e) {
        throw new UncheckedSystemException(e);
      }
      TransactionHelperImpl transactionHelperImpl = new TransactionHelperImpl();
      transactionHelperImpl.setTransactionManager(transactionManager);
      this.transactionHelper = transactionHelperImpl;
    }
  }
  // private Jotm jotm;

  private static Jotm jotm;

  @DataPoints
  public static TransactionManagerWithHelper[] transactionManagersWithHelpers;

  @AfterClass
  public static void afterClass() {
    jotm.stop();
  }

  @BeforeClass
  public static void beforeClass() {
    transactionManagersWithHelpers = new TransactionManagerWithHelper[2];

    try {
      jotm = new Jotm(true, false);
    } catch (NamingException e) {
      throw new RuntimeException(e);
    }
    transactionManagersWithHelpers[0] =
        new TransactionManagerWithHelper(jotm.getTransactionManager());

    try {
      transactionManagersWithHelpers[1] =
          new TransactionManagerWithHelper(new GeronimoTransactionManager());
    } catch (XAException e) {
      throw new UncheckedXAException(e);
    }
  }

  @Theory
  public void testRollback(final TransactionManagerWithHelper tmh) {
    Map<String, String> managedMap = new ManagedMap<String, String>(
        new ReadCommitedTransactionalMap<>(null), tmh.transactionManager);

    managedMap.put("key0", "value0");
    tmh.transactionHelper.required(() -> {
      managedMap.put("key1", "value1");
      try {
        tmh.transactionHelper.requiresNew(() -> {
          managedMap.put("key1", "otherValue");
          throw new NumberFormatException();
        });
      } catch (NumberFormatException e) {
        Assert.assertEquals(0, e.getSuppressed().length);
        Assert.assertEquals("value1", managedMap.get("key1"));
      }
      return null;
    });
    Assert.assertEquals(2, managedMap.size());
  }

  @Theory
  public void testSuspendedTransactions(final TransactionManagerWithHelper tmh) {
    Map<String, String> managedMap = new ManagedMap<String, String>(
        new ReadCommitedTransactionalMap<>(null), tmh.transactionManager);
    managedMap.put("test1", "value1");
    tmh.transactionHelper.required(() -> {
      Assert.assertEquals("value1", managedMap.get("test1"));
      managedMap.put("test2", "value2");
      tmh.transactionHelper.requiresNew(() -> {
        Assert.assertFalse(managedMap.containsKey("test2"));
        managedMap.put("test3", "value3");
        tmh.transactionHelper.notSupported(() -> {
          Assert.assertFalse(managedMap.containsKey("test3"));
          return null;
        });
        return null;
      });
      Assert.assertTrue(managedMap.containsKey("test2"));
      Assert.assertTrue(managedMap.containsKey("test3"));
      return null;
    });
  }
}
