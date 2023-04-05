/**********************************************************************
Copyright (c) 2004 Ralf Ulrich and others.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.



Contributors:
2004 Andy Jefferson - set useUpdateLock
    ...
**********************************************************************/
package org.zoodb.test.zoo.concurency;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.jdo.Extent;
import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;

import org.junit.Test;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.tools.ZooHelper;


/**
 * Tests for concurrent operations operating correctly and in the correct order.
 */
public class ConcurrencyTest {

    private static boolean initialised = false;

    public ConcurrencyTest() {
        if (!initialised) {
            initialised = true;
        }
    }

    private PersistenceManagerFactory pmf;

    private ZooJdoProperties props;

    @Test
    public void main() {
        String dbName = "concurrency.zdb";
        createDB(dbName);

        props = new ZooJdoProperties(dbName);
        pmf = JDOHelper.getPersistenceManagerFactory(props);
        pmf.setMultithreaded(true);

        try {
            testBasicConcurrency();
        } finally {
            pmf.close();
        }
    }

    static class LOG {
        public static void debug(String msg) {
            System.out.println(msg);
        }

        public static void error(String msg, Throwable thr) {
            System.out.println(msg + "\n" + thr.getMessage());
        }
    }

    private static void createDB(String dbName) {
        // remove database if it exists
        if (ZooHelper.dbExists(dbName)) {
            ZooHelper.removeDb(dbName);
        }

        // create database
        // By default, all database files will be created in %USER_HOME%/zoodb
        ZooHelper.createDb(dbName);
    }

    private static void closePm(PersistenceManager pm) {
        pm.close();
    }

    private PersistenceManager getPm() {
        PersistenceManager pm = pmf.getPersistenceManager();
        pm.setMultithreaded(true);
        return pm;
    }

    private void testBasicConcurrency() {
        // Persist Accounts and Transfers
        PersistenceManager pm = getPm();
        Transaction tx = pm.currentTransaction();
        try
        {
            tx.begin();
            pm.makePersistent(new Account("alice", 1000));
            pm.makePersistent(new Account("berta", 0));
            pm.makePersistent(new Account("charly", 0));
            pm.makePersistent(new Transfer("alice", "berta", 100));
            pm.makePersistent(new Transfer("alice", "charly", 200));
            tx.commit();
        }
        finally
        {
            if (tx.isActive())
            {
                tx.rollback();
            }
            closePm(pm);
        }

        // Process the Transfers, one per Thread, and one PM per Thread
        pm = getPm();
        tx = pm.currentTransaction();
        try
        {
            tx.begin();
            LinkedList<Thread> threads = new LinkedList<>();
            List<Object> objIds = new ArrayList<>();
            Extent<Transfer> ext = pm.getExtent(Transfer.class, true);
            Iterator<Transfer> iter = ext.iterator();
            while (iter.hasNext()) {
                Transfer t = iter.next();
                //threads.add(startConcurrentTransfer(JDOHelper.getObjectId(t)));
                objIds.add(JDOHelper.getObjectId(t));
            }
            tx.commit();

            for (Object id : objIds) threads.add(startConcurrentTransfer(id));

            while (!threads.isEmpty())
            {
                Thread td = threads.removeFirst();
                try
                {
                    td.join();
                }
                catch (InterruptedException e)
                {
                }
            }
        }
        finally
        {
            if (tx.isActive())
            {
                tx.rollback();
            }
            closePm(pm);
        }

        // Check the results
        pm = getPm();
        tx = pm.currentTransaction();
        try
        {
            tx.begin();

            Extent<Transfer> ext = pm.getExtent(Transfer.class, true);
            Iterator<Transfer> iter = ext.iterator();
            while (iter.hasNext())
            {
                Transfer transfer = iter.next();
                transfer.zooActivateRead();
                assertTrue(transfer.isBooked());
            }

            Extent<Account> extA = pm.getExtent(Account.class, true);
            Iterator<Account> iterA = extA.iterator();
            while (iterA.hasNext())
            {
                Account acct = iterA.next();
                acct.zooActivateRead();
                String name = acct.getName();
                if ("alice".equals(name))
                {
                    assertEquals(700, acct.getSaldo());
                }
                else if ("berta".equals(name))
                {
                    assertEquals(100, acct.getSaldo());
                }
                else if ("charly".equals(name))
                {
                    assertEquals(200, acct.getSaldo());
                }
                else
                {
                    assertFalse("unexpected account name: " + name, true);
                }
            }
            tx.commit();
        }
        finally
        {
            if (tx.isActive())
            {
                tx.rollback();
            }
            closePm(pm);
        }
    }

    private Thread startConcurrentTransfer(final Object transferId)
    {
        // Perform the transfer in its own PM
        final PersistenceManager pm = getPm();
        final Object lock = new Object();
        Thread td = new Thread()
        {
            @Override
            public void run()
            {
                LOG.debug(">> Starting thread " + transferId + " in pm=" + pm);
                synchronized (lock)
                {
                    lock.notifyAll();
                }

                pm.currentTransaction().begin();
                Transfer t = (Transfer) pm.getObjectById(transferId, true);

                performTransfer(t);
                LOG.debug(">> Completed thread " + transferId + " in pm=" + pm);
            }
        };

        synchronized (lock)
        {
            td.start();
            try
            {
                lock.wait();
            }
            catch (InterruptedException e)
            {
            }
        }
        return td;
    }

    public void performTransfer(Transfer t)
    {
        PersistenceManager pm = JDOHelper.getPersistenceManager(t);
        Transaction tx = pm.currentTransaction();
        t.zooActivateRead();
        int amount = t.getAmount();
        try
        {
            // Retrieve from and to Accounts using this PM - should lock the objects in the database
            Account from = getAccountByName(pm, t.getFromAccount());
            Account to = getAccountByName(pm, t.getToAccount());

            from.zooActivateRead();
            int fromSaldo = from.getSaldo() - amount;
            from.zooActivateWrite();
            from.setSaldo(fromSaldo);
            //pm.flush();
            try
            {
                // make sure the other transaction comes here concurrently
                Thread.sleep(1000);
            }
            catch (InterruptedException e)
            {
            }

            // Update "to" account
            to.zooActivateRead();
            int toSaldo = to.getSaldo() + amount;
            to.zooActivateWrite();
            to.setSaldo(toSaldo);

            // Update Transfer as "booked"
            t.zooActivateWrite();
            t.setBooked(true);

            tx.commit();
        }
        finally
        {
            if (tx.isActive())
            {
                tx.rollback();
            }
            closePm(pm);
        }
    }

    private static Account getAccountByName(PersistenceManager pm, String acct) {
        Query query = pm.newQuery("SELECT FROM " + Account.class.getName() + " WHERE name == :acct");
        @SuppressWarnings("unchecked")
        Collection<Account> result = (Collection<Account>) query.execute(acct);
        Iterator<Account> iter = result.iterator();
        Account account = iter.hasNext() ? iter.next() : null;
        query.closeAll();
        return account;
    }
}