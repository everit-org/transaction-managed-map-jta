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

import javax.transaction.xa.XAException;

import org.apache.geronimo.transaction.manager.GeronimoTransactionManager;
import org.everit.osgi.transaction.helper.api.TransactionHelper;
import org.everit.osgi.transaction.helper.internal.TransactionHelperImpl;
import org.everit.transaction.map.readcommited.ReadCommitedTransactionalMap;
import org.everit.transaction.unchecked.xa.UncheckedXAException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ManagedMapTest {

  private TransactionHelper transactionHelper = null;

  private GeronimoTransactionManager transactionManager;

  @Before
  public void before() {
    try {
      transactionManager = new GeronimoTransactionManager();
    } catch (XAException e) {
      throw new UncheckedXAException(e);
    }
    TransactionHelperImpl transactionHelperImpl = new TransactionHelperImpl();
    transactionHelperImpl.setTransactionManager(transactionManager);
    this.transactionHelper = transactionHelperImpl;
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
