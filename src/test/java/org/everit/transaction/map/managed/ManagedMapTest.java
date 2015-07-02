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

import org.everit.osgi.transaction.helper.api.TransactionHelper;
import org.everit.osgi.transaction.helper.internal.TransactionHelperImpl;
import org.everit.transaction.map.readcommited.ReadCommitedTransactionalMap;
import org.everit.transaction.unchecked.UncheckedSystemException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.objectweb.jotm.Jotm;

public class ManagedMapTest {

  private Jotm jotm;

  private TransactionHelper transactionHelper = null;

  private TransactionManager transactionManager;

  @After
  public void after() {
    jotm.stop();
  }

  @Before
  public void before() {
    try {
      this.jotm = new Jotm(true, false);
    } catch (NamingException e) {
      throw new RuntimeException();
    }

    transactionManager = jotm.getTransactionManager();
    try {
      transactionManager.setTransactionTimeout(6000);
    } catch (SystemException e) {
      throw new UncheckedSystemException(e);
    }

    TransactionHelperImpl transactionHelperImpl = new TransactionHelperImpl();
    transactionHelperImpl.setTransactionManager(transactionManager);
    this.transactionHelper = transactionHelperImpl;

  }

  @Test
  public void testRollback() {
    Map<String, String> managedMap = new ManagedMap<String, String>(
        new ReadCommitedTransactionalMap<>(null), transactionManager);

    managedMap.put("key0", "value0");
    transactionHelper.required(() -> {
      managedMap.put("key1", "value1");
      try {
        transactionHelper.requiresNew(() -> {
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

  @Test
  public void testSuspendedTransactions() {
    Map<String, String> managedMap = new ManagedMap<String, String>(
        new ReadCommitedTransactionalMap<>(null), transactionManager);
    managedMap.put("test1", "value1");
    transactionHelper.required(() -> {
      Assert.assertEquals("value1", managedMap.get("test1"));
      managedMap.put("test2", "value2");
      transactionHelper.requiresNew(() -> {
        Assert.assertFalse(managedMap.containsKey("test2"));
        managedMap.put("test3", "value3");
        transactionHelper.notSupported(() -> {
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
