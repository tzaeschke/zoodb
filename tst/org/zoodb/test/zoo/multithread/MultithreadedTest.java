/**********************************************************************
Copyright (c) 2013 Andy Jefferson and others. All rights reserved.
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

import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

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
 * Tests for JDO when run in a multithreaded environment (PersistenceManager per Thread).
 */
public class MultithreadedTest {

    private static final int THREAD_SIZE = 500;

    public MultithreadedTest() {
    }

    private PersistenceManagerFactory pmf;

    @Test
    public void main() {
        String dbName = "multithread.zdb";
        createDB(dbName);

        ZooJdoProperties props = new ZooJdoProperties(dbName);
        pmf = JDOHelper.getPersistenceManagerFactory(props);

        try {
            testQueryAndDetach();
            testFind();
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
     * Test that populates the datastore, and then starts many threads querying and detaching the data
     * and trying to access the detached data (checking for undetached fields that should have been detached).
     */
    private void testQueryAndDetach() {
        try {
            // Persist some data
            LOG.debug(">> Persisting data");
            PersistenceManager pm = getPm();
            Transaction tx = pm.currentTransaction();
            try {
                tx.begin();
                Manager mgr = new Manager(1, "The", "Boss", "the.boss@datanucleus.com", 200000, "100000");
                mgr.zooActivateWrite();
                pm.makePersistent(mgr);
                mgr.zooActivateRead();
                for (int i = 0; i < 100; i++) {
                    Employee emp = new Employee(i+2, "FirstName"+i, "LastName"+i,
                        "first.last." + i + "@datanucleus.com", 100000+i, "12345" + i);
                    emp.zooActivateWrite();
                    emp.setManager(mgr);
                    emp.zooActivateRead();

                    mgr.zooActivateWrite();
                    mgr.addSubordinate(emp);
                    pm.makePersistent(emp);
                }
                tx.commit();
            }
            catch (Throwable thr) {
                LOG.error("Exception persisting objects", thr);
                fail("Exception persisting data : " + thr.getMessage());
            }
            finally {
                if (tx.isActive()) {
                    tx.rollback();
                }
                closePm(pm);
            }
            LOG.debug(">> Persisted data");

            // Verify the persistence
            pm = getPm();
            tx = pm.currentTransaction();
            try {
                tx.begin();
                LOG.debug(">> Querying Employees");
                Query q = pm.newQuery(Employee.class);
                @SuppressWarnings("unchecked")
                Collection<Employee> emps = (Collection<Employee>) q.execute();
                for (Employee e : emps) {
                    e.zooActivateRead();
                    LOG.debug(">> emp=" + e + " e.mgr=" + e.getManager());
                }
                LOG.debug(">> Queried Employees");
                tx.commit();
            }
            catch (Throwable thr) {
                LOG.error("Exception checking objects", thr);
                fail("Exception checking data : " + thr.getMessage());
            }
            finally {
                if (tx.isActive()) {
                    tx.rollback();
                }
                closePm(pm);
            }

            // Create the Threads
            final String[] threadErrors = new String[THREAD_SIZE];
            Thread[] threads = new Thread[THREAD_SIZE];
            for (int i = 0; i < THREAD_SIZE; i++)
            {
                final int threadNo = i;
                threads[i] = new Thread(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        String errorMsg = processQueryAndDetach(true, threadNo);
                        threadErrors[threadNo] = errorMsg;
                    }
                });
            }

            // Run the Threads
            LOG.debug(">> testQueryAndDetach - Starting threads");
            for (int i = 0; i < THREAD_SIZE; i++)
            {
                threads[i].start();
            }
            for (int i = 0; i < THREAD_SIZE; i++)
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
            LOG.debug(">> testQueryAndDetach - Completed threads");

            // Process any errors and fail the test if any threads failed present
            for (String error : threadErrors)
            {
                if (error != null)
                {
                    fail(error);
                }
            }
        }
        finally {
            clearCompanyData();
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

    private String processQueryAndDetach(boolean transaction, int threadNo) {
        PersistenceManager pm = getPm();
        //pm.getFetchPlan().setGroup("all");
        pm.getFetchPlan().setMaxFetchDepth(-1);
        Transaction tx = pm.currentTransaction();
        try {
            if (transaction) {
                tx.begin();
            }
            Query q = pm.newQuery(Employee.class);
            @SuppressWarnings("unchecked")
            Collection<Employee> emps = (Collection<Employee>) q.execute();

            LOG.debug("Thread: " + threadNo);
            for (Employee e : emps) {
                try {
                    e.zooActivateRead();
                    LOG.debug(">> Employee: " + e.getFirstName() + " " + e.getLastName() + " bestFriend=" + e.getBestFriend());
                    if (e instanceof Manager) {
                        Set<Employee> subs = ((Manager)e).getSubordinates();
                        if (subs == null) {
                            return "Manager object didnt have its subordinates detached!";
                        }
                        else if (subs.size() != 100) {
                            return "Manager had " + subs.size() + " subordinates instead of 100";
                        }
                    }
                    else {
                        Manager mgr = e.getManager();
                        if (mgr == null) {
                            return "Employee=" + e + " didnt have its manager set!";
                        }
                        else {
                            mgr.zooActivateRead();
                            Set<Employee> subs = mgr.getSubordinates();
                            if (subs == null)
                            {
                                return "Employee=" + e + " didnt have its subordinates set!";
                            }
                            else if (subs.size() != 100)
                            {
                                return "Employee=" + e + " has Manager with " + subs.size() + " subordinates instead of 100";
                            }
                            for (Employee subE : subs)
                            {
                                subE.toString();
                            }
                        }
                    }
                }
                catch (Exception exc) {
                    LOG.error(">> Exception thrown on check of results", exc);
                    return "Exception thrown : " + exc.getMessage();
                }
            }

            if (transaction) {
                tx.commit();
            }
        }
        catch (Throwable thr) {
            LOG.error("Exception query objects", thr);
            return "Exception in query : " + thr.getMessage();
        }
        finally {
            if (transaction && tx.isActive()) {
                tx.rollback();
            }
            // Detached the Employees and their loaded fields
            closePm(pm);
        }

        return null;
    }

    /**
     * Test that populates the datastore, and then starts many threads retrieving objects.
     */
    private void testFind() {
        try {
            // Persist some data
            LOG.debug(">> Persisting data");
            PersistenceManager pm = getPm();
            Transaction tx = pm.currentTransaction();
            Object mgrId = null;
            try {
                tx.begin();
                Manager mgr = new Manager(1, "The", "Boss", "the.boss@datanucleus.com", 200000, "100000");
                mgr.zooActivateWrite();
                pm.makePersistent(mgr);
                for (int i=0; i<100; i++) {
                    Employee emp = new Employee(i+2, "FirstName"+i, "LastName"+i,
                        "first.last." + i + "@datanucleus.com", 100000+i, "12345" + i);
                    emp.zooActivateWrite();
                    emp.setManager(mgr);
                    mgr.zooActivateWrite();
                    mgr.addSubordinate(emp);
                    pm.makePersistent(emp);
                }

                tx.commit();
                mgrId = JDOHelper.getObjectId(mgr);
            }
            catch (Throwable thr) {
                LOG.error("Exception persisting objects", thr);
                fail("Exception persisting data : " + thr.getMessage());
            }
            finally {
                if (tx.isActive()) {
                    tx.rollback();
                }
                closePm(pm);
            }
            LOG.debug(">> Persisted data");

            // Verify the persistence
            pm = getPm();
            tx = pm.currentTransaction();
            try {
                tx.begin();
                Query q = pm.newQuery(Employee.class);
                @SuppressWarnings("unchecked")
                Collection<Employee> emps = (Collection<Employee>) q.execute();
                for (Employee e : emps) {
                    e.zooActivateRead();
                    LOG.debug(">> emp=" + e + " e.mgr=" + e.getManager());
                }
                LOG.debug(">> Queried Employees");
                tx.commit();
            }
            catch (Throwable thr) {
                LOG.error("Exception checking objects", thr);
                fail("Exception checking data : " + thr.getMessage());
            }
            finally {
                if (tx.isActive()) {
                    tx.rollback();
                }
                closePm(pm);
            }

            // Create the Threads
            final Object managerId = mgrId;
            final String[] threadErrors = new String[THREAD_SIZE];
            Thread[] threads = new Thread[THREAD_SIZE];
            for (int i = 0; i < THREAD_SIZE; i++)
            {
                final int threadNo = i;
                threads[i] = new Thread(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        String errorMsg = processFind(managerId, true);
                        threadErrors[threadNo] = errorMsg;
                    }
                });
            }

            // Run the Threads
            LOG.debug(">> testFind - Starting threads");
            for (int i = 0; i < THREAD_SIZE; i++)
            {
                threads[i].start();
            }
            for (int i = 0; i < THREAD_SIZE; i++)
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
            LOG.debug(">> testFind - Completed threads");

            // Process any errors and fail the test if any threads failed present
            for (String error : threadErrors)
            {
                if (error != null)
                {
                    fail(error);
                }
            }
        }
        finally {
            // Clean out data
            clearCompanyData();
        }
    }

    private String processFind(Object mgrId, boolean transaction)
    {
        PersistenceManager pm = getPm();
        //pm.getPersistenceManagerFactory().getDataStoreCache().evictAll();
        Transaction tx = pm.currentTransaction();
        try {
            if (transaction) {
                tx.begin();
            }
            Manager mgr = (Manager)pm.getObjectById(mgrId);
            if (mgr == null)
            {
                return "Manager not found!";
            }
            mgr.zooActivateRead();
            if (!("The".equals(mgr.getFirstName())))
            {
                return "Manager first name is wrong";
            }
            if (!("Boss".equals(mgr.getLastName())))
            {
                return "Manager last name is wrong";
            }
            Set<Employee> emps = mgr.getSubordinates();
            if (emps == null)
            {
                return "Manager has null subordinates!";
            }
            else if (emps.size() != 100)
            {
                return "Manager has incorrect number of subordinates (" + emps.size() + ")";
            }

            if (transaction) {
                tx.commit();
            }
        }
        catch (Throwable thr) {
            LOG.error("Exception in find", thr);
            return "Exception in find : " + thr.getMessage();
        }
        finally {
            if (transaction && tx.isActive()) {
                tx.rollback();
            }
            closePm(pm);
        }
        return null;
    }
}