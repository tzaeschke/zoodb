/**********************************************************************
 Copyright (c) 2007 Andy Jefferson and others. All rights reserved.
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
package org.zoodb.test.zoo.generics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;

import org.junit.Test;
import org.zoodb.api.impl.ZooPC;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.tools.ZooHelper;

/**
 * Test use of Java generics.
 */
public class GenericsTest {

    public GenericsTest() {
    }

    private PersistenceManagerFactory pmf;

    @Test
    public void main() {
        String dbName = "generics.zdb";
        createDB(dbName);

        ZooJdoProperties props = new ZooJdoProperties(dbName);
        pmf = JDOHelper.getPersistenceManagerFactory(props);

        try {
            testCollectionMapGenerics();
            testInheritanceGenerics();
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
     * Test for using Java generics and not specifying <collection>, <map> in MetaData
     */
    private void testCollectionMapGenerics()
    {
        try
        {
            Object contId = null;

            PersistenceManager pm = getPm();
            Transaction tx = pm.currentTransaction();
            try
            {
                tx.begin();

                GenericsContainer cont = new GenericsContainer("MyContainer");

                GenericsElement elem1 = new GenericsElement("FirstElement");
                cont.zooActivateWrite();
                cont.addElement(elem1);
                GenericsElement elem2 = new GenericsElement("SecondElement");
                cont.addElement(elem2);

                GenericsValue val1 = new GenericsValue("FirstValue");
                cont.addEntry("1", val1);

                pm.makePersistent(cont);

                tx.commit();
                contId = JDOHelper.getObjectId(cont);
            }
            finally
            {
                if (tx.isActive())
                {
                    tx.rollback();
                }
                closePm(pm);
            }

            // Check what was persisted
            pm = getPm();
            tx = pm.currentTransaction();
            try
            {
                tx.begin();
                GenericsContainer cont = (GenericsContainer) pm.getObjectById(contId);
                cont.zooActivateRead();
                HashSet<GenericsElement> elements = cont.getElements();
                HashMap<String, GenericsValue> valueMap = cont.getValueMap();

                assertNotNull("Elements in container was null but should have had 2 elements", elements);
                assertEquals("Number of elements in container is incorrect", 2, elements.size());

                assertNotNull("ValueMap in container was null but should have had 1 entry", valueMap);
                assertEquals("Number of entries in value map in container is incorrect", 1, valueMap.size());

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
        finally
        {
            // Clean out our data
            PersistenceManager pm = getPm();
            try {
                cleanClassForPM(pm, GenericsContainer.class);
                cleanClassForPM(pm, GenericsElement.class);
                cleanClassForPM(pm, GenericsValue.class);
            } finally {
                closePm(pm);
            }
        }
    }

    /**
     * Test for using Java generics in the inheritance tree.
     */
    private void testInheritanceGenerics()
    {
        try
        {
            PersistenceManager pm = getPm();
            Transaction tx = pm.currentTransaction();
            try
            {
                tx.begin();

                GenericsBaseSubSub sub = new GenericsBaseSubSub();
                sub.zooActivateWrite();
                sub.setId(1l);
                sub.setName("First Sub");
                sub.setLongValue(101l);
                GenericsBaseSubRelated rel = new GenericsBaseSubRelated();
                rel.zooActivateWrite();
                rel.setId(150);
                rel.setBaseSub(sub);
                sub.setRelated(rel);
                pm.makePersistent(rel);

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

            pm = getPm();
            tx = pm.currentTransaction();
            try
            {
                tx.begin();

                Query q = pm.newQuery(GenericsBase.class);
                @SuppressWarnings({ "rawtypes", "unchecked" })
                Collection<GenericsBase> results = (Collection<GenericsBase>) q.execute();
                assertEquals(1, results.size());
                @SuppressWarnings("rawtypes")
                GenericsBase base = results.iterator().next();
                base.zooActivateRead();
                assertNotNull(base);
                assertTrue(base instanceof GenericsBaseSubSub);
                GenericsBaseSubSub sub = (GenericsBaseSubSub)base;
                assertEquals("First Sub", sub.getName());
                assertEquals(101l, sub.getLongValue());
                assertEquals(new Long(1), sub.getId());
                GenericsBaseSubRelated rel = sub.getRelated();
                rel.zooActivateRead();
                assertNotNull(rel);
                assertEquals(150, rel.getId());
                assertEquals(rel.getBaseSub(), sub);

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
        finally
        {
            // Clean out our data
            PersistenceManager pm = getPm();
            try {
                cleanClassForPM(pm, GenericsBaseSubRelated.class);
                cleanClassForPM(pm, GenericsBaseSubSub.class);
            } finally {
                closePm(pm);
            }
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void cleanClassForPM(PersistenceManager pm, Class cls)
    {
        Transaction tx = pm.currentTransaction();
        tx.setOptimistic(false);

        try
        {
            // delete all objects of this class (and subclasses)
            tx.begin();

            Query q = pm.newQuery(cls);
            Collection results = (Collection) q.execute();
            Iterator iter = results.iterator();
            Collection coll = new HashSet();
            while (iter.hasNext())
            {
                Object obj = iter.next();
                ((ZooPC) obj).zooActivateRead();
                LOG.debug("Cleanup object to delete=" + obj.toString() + " state=" + JDOHelper.getObjectState(obj));
                coll.add(obj);
            }
            LOG.debug("Cleanup : Number of objects of type " + cls.getName() + " to delete is " + coll.size());
            pm.deletePersistentAll(coll);
            LOG.debug("Cleanup : Number of objects deleted is " + coll.size());

            tx.commit();
        }
        catch (RuntimeException e)
        {
            LOG.error("Exception in clean", e);
            throw e;
        }
    }
}