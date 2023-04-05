/**********************************************************************
Copyright (c) 2005 Erik Bengtson and others. All rights reserved.
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
    ...
**********************************************************************/
package org.zoodb.test.zoo.multithread;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Iterator;
import java.util.Set;

import javax.jdo.Extent;
import javax.jdo.FetchPlan;
import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Transaction;

import org.junit.Test;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.tools.ZooHelper;

/**
 * Tests for multithreading capabilities.
 * Note that this tests multi-threaded PM capabilities, not multi-threaded PMF capabilities.
 */
public class MultithreadPMTest {

    private static final int THREAD_SIZE = 1000;

    private static boolean initialised = false;

    public MultithreadPMTest()
    {
        if (!initialised) {
            initialised = true;
        }
    }

    private PersistenceManagerFactory pmf;

    @Test
    public void main() {
        String dbName = "multithreadpm.zdb";
        createDB(dbName);

        ZooJdoProperties props = new ZooJdoProperties(dbName);
        pmf = JDOHelper.getPersistenceManagerFactory(props);

        try {
            testMultipleTransitionRead();
            testMultipleNonTransactionalRead();
            testMultipleTransitionWrite();
            testEvictAllAndWrites();

            //testMultipleNonTransitionWrite();
            //testMultipleDetachCopy();
            //testMultipleDetachCopyAndFetchPlanModification();
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

    /**
     * Test changing the state
     */
    private void testMultipleTransitionRead()
    {
        try
        {
            Thread[] threads = new Thread[THREAD_SIZE];

            PersistenceManager pm = getPm();
            pm.currentTransaction().begin();
            final Employee woody = new Employee(1,"Woody","Woodpecker","woody@woodpecker.com",13,"serial 1",new Integer(10));
            Manager bart = new Manager(2,"Bart","Simpson","bart@simpson.com",2,"serial 2");
            woody.zooActivateWrite();
            woody.setManager(bart);
            pm.makePersistent(woody);
            pm.currentTransaction().commit();
            pm.currentTransaction().begin();
            try
            {
                for (int i=0; i<THREAD_SIZE; i++)
                {
                    threads[i] = new Thread( new Runnable()
                        {
                        @Override
                        public void run()
                        {
                            woody.zooActivateRead();
                            woody.getLastName();
                            Manager mgr = woody.getManager();
                            mgr.zooActivateRead();
                            mgr.getLastName();
                        }
                        });
                }
                for (int i=0; i<THREAD_SIZE; i++)
                {
                    threads[i].start();
                }
                for (int i=0; i<THREAD_SIZE; i++)
                {
                    try
                    {
                        threads[i].join();
                    }
                    catch (InterruptedException e)
                    {
                        fail(e.getMessage());
                    }
                }
            }
            finally
            {
                if (pm.currentTransaction().isActive())
                {
                    pm.currentTransaction().rollback();
                }
                closePm(pm);
            }
        }
        finally
        {
            clearCompanyData();
        }
    }

    /**
     * Test changing the state
     */
    private void testMultipleNonTransactionalRead()
    {
        try
        {
            Thread[] threads = new Thread[THREAD_SIZE];

            PersistenceManager pm = getPm();
            pm.currentTransaction().begin();
            final Employee woody = new Employee(1,"Woody","Woodpecker","woody@woodpecker.com",13,"serial 1",new Integer(10));
            Manager bart = new Manager(2,"Bart","Simpson","bart@simpson.com",2,"serial 2");
            woody.zooActivateWrite();
            woody.setManager(bart);
            pm.makePersistent(woody);
            pm.currentTransaction().commit();
            pm.currentTransaction().setNontransactionalRead(true);
            try
            {
                for (int i=0; i<THREAD_SIZE; i++)
                {
                    threads[i] = new Thread( new Runnable()
                        {
                        @Override
                        public void run()
                        {
                            woody.zooActivateRead();
                            woody.getLastName();
                             Manager mgr = woody.getManager();
                             mgr.zooActivateRead();
                             mgr.getLastName();
                        }
                        });
                }
                for (int i=0; i<THREAD_SIZE; i++)
                {
                    threads[i].start();
                }
                for (int i=0; i<THREAD_SIZE; i++)
                {
                    try
                    {
                        threads[i].join();
                    }
                    catch (InterruptedException e)
                    {
                        fail(e.getMessage());
                    }
                }
            }
            finally
            {
                if (pm.currentTransaction().isActive())
                {
                    pm.currentTransaction().rollback();
                }
                closePm(pm);
            }
        }
        finally
        {
            clearCompanyData();
        }
    }

    /**
     * Test changing the state
     */
    private void testMultipleTransitionWrite()
    {
        try
        {
            Thread[] threads = new Thread[THREAD_SIZE];

            PersistenceManager pm = getPm();
            pm.currentTransaction().begin();
            final Employee woody = new Employee(1,"Woody","Woodpecker","woody@woodpecker.com",13,"serial 1",new Integer(10));
            Manager bart = new Manager(2,"Bart","Simpson","bart@simpson.com",2,"serial 2");
            final Manager boss = new Manager(3,"Boss","WakesUp","boss@wakes.up",4,"serial 3");
            woody.zooActivateWrite();
            woody.setManager(bart);
            pm.makePersistent(woody);
            pm.currentTransaction().commit();
            pm.currentTransaction().begin();
            try
            {
                for (int i=0; i<THREAD_SIZE; i++)
                {
                    threads[i] = new Thread( new Runnable()
                        {
                        @Override
                        public void run()
                        {
                            woody.zooActivateWrite();
                            woody.setLastName("name");
                            woody.setManager(boss);
                        }
                        });
                }
                for (int i=0; i<THREAD_SIZE; i++)
                {
                    threads[i].start();
                }
                for (int i=0; i<THREAD_SIZE; i++)
                {
                    try
                    {
                        threads[i].join();
                    }
                    catch (InterruptedException e)
                    {
                        fail(e.getMessage());
                    }
                }
            }
            finally
            {
                if (pm.currentTransaction().isActive())
                {
                    pm.currentTransaction().rollback();
                }
                closePm(pm);
            }
        }
        finally
        {
            clearCompanyData();
        }
    }

    /**
     * Test changing the state
     */
    private void testEvictAllAndWrites()
    {
        try
        {
            Thread[] threads = new Thread[THREAD_SIZE];
            Thread[] threads2 = new Thread[THREAD_SIZE];

            final PersistenceManager pm = getPm();
            pm.currentTransaction().begin();
            final Employee woody = new Employee(1,"Woody","Woodpecker","woody@woodpecker.com",13,"serial 1",new Integer(10));
            Manager bart = new Manager(2,"Bart","Simpson","bart@simpson.com",2,"serial 2");
            final Manager boss = new Manager(3,"Boss","WakesUp","boss@wakes.up",4,"serial 3");
            woody.zooActivateWrite();
            woody.setManager(bart);
            pm.makePersistent(woody);
            final Object id = pm.getObjectId(woody);
            pm.currentTransaction().commit();
            pm.currentTransaction().begin();
            try
            {
                for (int i=0; i<THREAD_SIZE; i++)
                {
                    threads[i] = new Thread( new Runnable()
                        {
                        @Override
                        public void run()
                        {
                            pm.getObjectById(id, true);
                            woody.zooActivateWrite();
                            woody.setLastName("name");
                            woody.setManager(boss);
                        }
                        });
                }
                for (int i=0; i<THREAD_SIZE; i++)
                {
                    threads2[i] = new Thread( new Runnable()
                        {
                        @Override
                        public void run()
                        {
                            pm.evictAll();
                        }
                        });
                }
                for (int i=0; i<THREAD_SIZE; i++)
                {
                    threads[i].start();
                    threads2[i].start();
                }
                for (int i=0; i<THREAD_SIZE; i++)
                {
                    try
                    {
                        threads[i].join();
                        threads2[i].join();
                    }
                    catch (InterruptedException e)
                    {
                        fail(e.getMessage());
                    }
                }
            }
            finally
            {
                if (pm.currentTransaction().isActive())
                {
                    pm.currentTransaction().rollback();
                }
                closePm(pm);
            }
        }
        finally
        {
            clearCompanyData();
        }
    }

    /**
     * Test changing the state
     */
    private void testMultipleNonTransitionWrite()
    {
        try
        {
            Thread[] threads = new Thread[THREAD_SIZE];

            PersistenceManager pm = getPm();
            pm.currentTransaction().begin();
            final Employee woody = new Employee(1,"Woody","Woodpecker","woody@woodpecker.com",13,"serial 1",new Integer(10));
            Manager bart = new Manager(2,"Bart","Simpson","bart@simpson.com",2,"serial 2");
            final Manager boss = new Manager(3,"Boss","WakesUp","boss@wakes.up",4,"serial 3");
            woody.zooActivateWrite();
            woody.setManager(bart);
            pm.makePersistent(woody);
            pm.currentTransaction().commit();
            pm.currentTransaction().setNontransactionalWrite(true);
            try
            {
                for (int i=0; i<THREAD_SIZE; i++)
                {
                    threads[i] = new Thread( new Runnable()
                        {
                        @Override
                        public void run()
                        {
                            woody.zooActivateWrite();
                            woody.setLastName("name");
                            woody.setManager(boss);
                        }
                        });
                }
                for (int i=0; i<THREAD_SIZE; i++)
                {
                    threads[i].start();
                }
                for (int i=0; i<THREAD_SIZE; i++)
                {
                    try
                    {
                        threads[i].join();
                    }
                    catch (InterruptedException e)
                    {
                        fail(e.getMessage());
                    }
                }
            }
            finally
            {
                if (pm.currentTransaction().isActive())
                {
                    pm.currentTransaction().rollback();
                }
                closePm(pm);
            }
        }
        finally
        {
            clearCompanyData();
        }
    }

    private void testMultipleDetachCopy()
    {
        try
        {
            Thread[] threads = new Thread[THREAD_SIZE];
            MultithreadDetachRunner[] runner = new MultithreadDetachRunner[THREAD_SIZE];

            PersistenceManager pm = getPm();
            pm.currentTransaction().begin();
            Employee woody = new Employee(1,"Woody","Woodpecker","woody@woodpecker.com",13,"serial 1",new Integer(10));
            Manager bart = new Manager(2,"Bart","Simpson","bart@simpson.com",2,"serial 2");
            woody.zooActivateWrite();
            woody.setManager(bart);
            pm.makePersistent(woody);
            pm.currentTransaction().commit();

            pm.currentTransaction().begin();
            try
            {
                for (int i=0; i<THREAD_SIZE; i++)
                {
                    runner[i] = new MultithreadDetachRunner(pm, woody);
                    threads[i] = new Thread(runner[i]);
                    threads[i].start();
                }
                for (int i=0; i<THREAD_SIZE; i++)
                {
                    threads[i].join();
                    if (runner[i].getException() != null)
                    {
                        LOG.error("Exception during test", runner[i].getException());
                        fail("Exception thrown during test : " + runner[i].getException());
                    }
                }
            }
            catch (Exception e)
            {
                fail(e.getMessage());
            }
            finally
            {
                if (pm.currentTransaction().isActive())
                {
                    pm.currentTransaction().rollback();
                }
                closePm(pm);
            }
        }
        finally
        {
            clearCompanyData();
        }
    }

    private class MultithreadDetachRunner implements Runnable
    {
        PersistenceManager threadPM;
        Object obj;
        Exception exception = null;

        public MultithreadDetachRunner(PersistenceManager pm, Object obj)
        {
            this.threadPM = pm;
            this.obj = obj;
        }

        public Exception getException()
        {
            return exception;
        }

        @Override
        public void run()
        {
            Employee woody = (Employee) obj;
            Employee woodyDetached = null;
            try
            {
                //test detach and attach
                threadPM.getFetchPlan().addGroup("groupA");
                threadPM.getFetchPlan().setDetachmentOptions(FetchPlan.DETACH_LOAD_FIELDS);
//                pm.getFetchPlan().removeGroup(FetchPlan.DEFAULT);
                woodyDetached = threadPM.detachCopy(woody);
                assertEquals(woody.getLastName(), woodyDetached.getLastName());
                assertEquals(woody.getManager().getLastName(), woodyDetached.getManager().getLastName());
            }
            catch (Exception e)
            {
                exception = e;
                LOG.error("Exception thrown during detachCopy process", e);
            }
        }
    }

    private void testMultipleDetachCopyAndFetchPlanModification()
    {
        try
        {
            Thread[] threads = new Thread[THREAD_SIZE];
            Runnable[] runner = new Runnable[THREAD_SIZE];

            PersistenceManager pm = getPm();
            pm.currentTransaction().begin();
            Employee woody = new Employee(1,"Woody","Woodpecker","woody@woodpecker.com",13,"serial 1",new Integer(10));
            Manager bart = new Manager(2,"Bart","Simpson","bart@simpson.com",2,"serial 2");
            woody.zooActivateWrite();
            woody.setManager(bart);
            pm.makePersistent(woody);
            pm.currentTransaction().commit();

            pm.currentTransaction().begin();
            try
            {
                for (int i=0; i<THREAD_SIZE; i++)
                {
                    if (i % 2 == 0)
                    {
                        runner[i] = new MultithreadDetachRunner(pm,woody);
                    }
                    else
                    {
                        runner[i] = new MultithreadFetchPlanRunner(pm);
                    }
                    threads[i] = new Thread(runner[i]);
                    threads[i].start();
                }
                for (int i=0; i<THREAD_SIZE; i++)
                {
                    threads[i].join();

                    Exception e = null;
                    if (runner[i] instanceof MultithreadDetachRunner)
                    {
                        e = ((MultithreadDetachRunner)runner[i]).getException();
                    }
                    else if (runner[i] instanceof MultithreadFetchPlanRunner)
                    {
                        e = ((MultithreadFetchPlanRunner)runner[i]).getException();
                    }
                    if (e != null)
                    {
                        LOG.error("Exception during test", e);
                        fail("Exception thrown during test : " + e);
                    }
                }
            }
            catch (Exception e)
            {
                fail(e.getMessage());
            }
            finally
            {
                if (pm.currentTransaction().isActive())
                {
                    pm.currentTransaction().rollback();
                }
                closePm(pm);
            }
        }
        finally
        {
            clearCompanyData();
        }
    }

    private class MultithreadFetchPlanRunner implements Runnable
    {
        PersistenceManager pm;
        Exception exception = null;

        public MultithreadFetchPlanRunner(PersistenceManager pm)
        {
            this.pm = pm;
        }

        public Exception getException()
        {
            return exception;
        }

        @Override
        public void run()
        {
            try
            {
                // clear the fetch groups
                pm.getFetchPlan().clearGroups();
            }
            catch (Exception e)
            {
                LOG.error("Exception during clear of groups on FetchPlan", e);
                fail(e.toString());
            }
        }
    }

    /**
     * Convenience method to clean out all Company data
     */
    private void clearCompanyData() {
        PersistenceManager pm = getPm();
        Transaction tx = pm.currentTransaction();

        try  {
            // disassociate all Employees and Departments from their Managers
            tx.begin();

            {
                Extent<Manager> ext = pm.getExtent(Manager.class, false);
                Iterator<Manager> it = ext.iterator();
                while (it.hasNext()) {
                    Manager mgr = it.next();
                    mgr.zooActivateRead();
                    Set<Employee> subordinates = mgr.getSubordinates();
                    if (subordinates != null && subordinates.size() > 0)
                    {
                        Iterator<Employee> empIter = subordinates.iterator();
                        while (empIter.hasNext()) {
                            Employee emp = empIter.next();
                            emp.zooActivateRead();
                            emp.zooActivateWrite();
                            emp.setManager(null);
                        }
                        mgr.zooActivateWrite();
                        mgr.clearSubordinates();
                    }

                    Iterator<Department> deptIter = mgr.getDepartments().iterator();
                    while (deptIter.hasNext()) {
                        Department dept = deptIter.next();
                        dept.zooActivateRead();
                        dept.zooActivateWrite();
                        dept.setManager(null);
                        dept.getProjects().clear();
                    }
                    mgr.zooActivateWrite();
                    mgr.clearDepartments();
                }
                tx.commit();
            }

            {
                tx.begin();
                Extent<Employee> ext = pm.getExtent(Employee.class, true);
                Iterator<Employee> it = ext.iterator();
                while (it.hasNext()) {
                    Employee emp = it.next();
                    emp.zooActivateRead();
                    emp.zooActivateWrite();
                    emp.setAccount(null);
                    emp.setManager(null);
                }
                tx.commit();
            }

            {
                tx.begin();
                Extent<Person> ext = pm.getExtent(Person.class, true);
                Iterator<Person> it = ext.iterator();
                while (it.hasNext()) {
                    Person pers = it.next();
                    pers.zooActivateRead();
                    pers.zooActivateWrite();
                    pers.setBestFriend(null); // Clear link to best friend
                }
                tx.commit();
            }

            {
                // disassociate the Qualification and Person objects
                tx.begin();
                Extent<Qualification> ext = pm.getExtent(Qualification.class, false);
                Iterator<Qualification> it = ext.iterator();
                while (it.hasNext()) {
                    Qualification q = it.next();
                    q.zooActivateRead();
                    q.zooActivateWrite();
                    q.setPerson(null);
                }
                tx.commit();
            }

            {
                // disassociate the Manager objects
                tx.begin();
                Extent<Manager> ext = pm.getExtent(Manager.class, false);
                Iterator<Manager> it = ext.iterator();
                while (it.hasNext()) {
                    Manager mgr = it.next();
                    mgr.zooActivateRead();
                    mgr.zooActivateWrite();
                    mgr.setManager(null);
                }
                tx.commit();
            }

            {
                // disassociate the Departments from the Offices
                tx.begin();
                Extent<Office> ext = pm.getExtent(Office.class, false);
                Iterator<Office> it = ext.iterator();
                while (it.hasNext()) {
                    Office off = it.next();
                    off.zooActivateRead();
                    off.zooActivateWrite();
                    off.clearDepartments();
                }
                tx.commit();
            }

        }
        catch (Exception e) {
            LOG.error("Exception thrown during clear", e);
            fail("Exception thrown during clear() : " + e.getMessage());
        }
        finally {
            if (tx.isActive()) {
                tx.rollback();
            }
            closePm(pm);
        }
    }
}