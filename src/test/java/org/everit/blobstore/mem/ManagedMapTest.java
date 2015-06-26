package org.everit.blobstore.mem;

import java.util.Map;

import javax.transaction.xa.XAException;

import org.apache.geronimo.transaction.manager.GeronimoTransactionManager;
import org.everit.osgi.transaction.helper.api.TransactionHelper;
import org.everit.osgi.transaction.helper.internal.TransactionHelperImpl;
import org.everit.transaction.managed.map.jta.ManagedMap;
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
    Map<String, String> managedMap = new ManagedMap<String, String>(transactionManager);
    managedMap.put("test1", "value1");
    transactionHelper.required(() -> {
      Assert.assertEquals("value1", managedMap.get("test1"));
      managedMap.put("test2", "value2");
      transactionHelper.requiresNew(() -> {
        Assert.assertFalse(managedMap.containsKey("test2"));
        managedMap.put("test3", "value3");
        return null;
      });
      Assert.assertTrue(managedMap.containsKey("test2"));
      Assert.assertTrue(managedMap.containsKey("test3"));
      return null;
    });
  }
}
