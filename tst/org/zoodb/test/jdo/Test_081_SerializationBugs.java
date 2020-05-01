/*
 * Copyright 2009-2020 Tilmann Zaeschke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zoodb.test.jdo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.test.testutil.TestTools;

public class Test_081_SerializationBugs {


    @Before
    public void before() {
        // nothing
    }


    /**
     * Run after each test.
     */
    @After
    public void after() {
        TestTools.closePM();
    }


    @BeforeClass
    public static void beforeClass() {
        TestTools.createDb();
        TestTools.defineSchema(PersistentTwitData.class, PaperPage.class, TwitInternalImpl.class);
        TestTools.defineSchema(SmallMap.class, SmallKey.class);
    }


    @AfterClass
    public static void afterClass() {
        TestTools.removeDb();
    }


    /**
     * Test serialisation. 
     */
    @Test
    public void testSerialization() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();
        PersistentTwitData map2 = new PersistentTwitData();
        Set<TwitInternal> set1 = new HashSet<TwitInternal>();
        set1.add(new TwitInternalImpl());
        map2.twitsByPaperPage().put(new PaperPage("2", 1), set1);
        pm.makePersistent(map2);
        Object oid2 = pm.getObjectId(map2);
        pm.currentTransaction().commit();
        TestTools.closePM();

        //check target
        pm = TestTools.openPM();
        pm.currentTransaction().begin();

        //Check for content in target
        map2 = (PersistentTwitData) pm.getObjectById(oid2, true);
        pm.currentTransaction().rollback();
        TestTools.closePM();
    }


    /**
     * Test serialisation. 
     * This tests a bug that caused a ConcurrentModificationException in the de-serializer
     * while de-serializing a SmallMap instance. 
     * The problem is that in the end of the de-serialization, the map is filled with persistent
     * object. When their hashcode/equals method is called, another de-serialization is triggered
     * (because the key/value is not in the cache yet). This second de-serialization end up using
     * the same instance of the de-serializer, resulting in concurrent-modification exception
     * when iterating over the member 'mapsToFill'.
     * Fix:
     * - key in cache -> otherwise double de-serialization
     * - non re-entrant Deserializer, otherwise mapps are filled twice (not a functional bug, but a
     *   performance problem.
     */
    @Test
    public void testSerializationSmall() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();
        SmallMap map1 = new SmallMap();
        map1.map().put(new SmallKey(), null);
        pm.makePersistent(map1);
        Object oid1 = pm.getObjectId(map1);
        pm.currentTransaction().commit();
        TestTools.closePM();

        //check target
        pm = TestTools.openPM();
        pm.currentTransaction().begin();

        //Check for content in target
        map1 = (SmallMap) pm.getObjectById(oid1, true);
        pm.currentTransaction().rollback();
        TestTools.closePM();
    }
}


class SmallMap extends PersistenceCapableImpl {
    HashMap<SmallKey, SmallKey> map = new HashMap<SmallKey, SmallKey>();

    public Map<SmallKey, SmallKey> map() {
        zooActivateWrite();
        return map;
    }
}

class SmallKey extends PersistenceCapableImpl {
    int l;
    
    @Override
    public int hashCode() {
        zooActivateRead();
        return super.hashCode();
    }
    @Override
    public boolean equals(Object obj) {
        zooActivateRead();
        return super.equals(obj);
    }
}

class Tag {

}

interface TwitInternal {
}


class ReceivedTwitImpl {

}

class PersistentTwitData extends PersistenceCapableImpl {

    private long nextId;
    private final Map<String, Tag> tagsByName;
    private final Map<Tag, Stack<TwitInternal>> twitsByTag;
    private final List<TwitInternal> twits;
    private final Map<PaperPage,Set<TwitInternal>> twitsByPaperPage;
    private final List<ReceivedTwitImpl> receivedTwits;


    public PersistentTwitData() {
        //	      this.tagsByName = new DBHashMap<String, TagImpl>();
        //	      this.textsByTag = new DBHashMap<TagImpl, Stack<String>>();
        this.tagsByName = new HashMap<String, Tag>();
        this.twitsByTag = new HashMap<Tag, Stack<TwitInternal>>();
        this.twits = new ArrayList<TwitInternal>();
        this.twitsByPaperPage = new HashMap<PaperPage, Set<TwitInternal>>();
        this.receivedTwits = new ArrayList<ReceivedTwitImpl>();
        this.nextId = 0;
    }

    public Map<String, Tag> tagsByName() {
        //	      this.zooActivateRead();
        this.zooActivateWrite();
        return this.tagsByName;
    }

    public Map<Tag, Stack<TwitInternal>> twitsByTag() {
        //	      this.zooActivateRead();
        this.zooActivateWrite();
        return this.twitsByTag;
    }

    public Map<PaperPage, Set<TwitInternal>> twitsByPaperPage() {
        this.zooActivateWrite();
        return this.twitsByPaperPage;
    }

    public List<TwitInternal> twits() {
        this.zooActivateWrite();
        return this.twits;
    }

    public long nextId() {
        this.zooActivateWrite();
        final long value = this.nextId;
        this.nextId++;
        return value;
    }

    public List<ReceivedTwitImpl> receivedTwits() {
        this.zooActivateWrite();
        return this.receivedTwits;
    }

}

class PaperPage extends PersistenceCapableImpl {

    private String documentID;
    private int pageNumber;

    @SuppressWarnings("unused")
    private PaperPage() { 
        //private, for ZooDB
    }

    public PaperPage(final String documentID, final int pageNumber) {
        this.documentID = documentID;
        this.pageNumber = pageNumber;
    }

    @Override
    public int hashCode() {
        this.zooActivateRead();
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.documentID == null ? 0 : this.documentID.hashCode());
        result = prime * result + this.pageNumber;
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        this.zooActivateRead();
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof PaperPage)) {
            return false;
        }
        final PaperPage other = (PaperPage) obj;
        if (this.documentID == null) {
            if (other.documentID != null) {
                return false;
            }
        } else if (!this.documentID.equals(other.documentID)) {
            return false;
        }
        if (this.pageNumber != other.pageNumber) {
            return false;
        }
        return true;
    }

}

class TwitInternalImpl extends PersistenceCapableImpl implements TwitInternal {

    @SuppressWarnings("unused")
    private Set<Tag> tags;

    TwitInternalImpl() {
        this.tags = new HashSet<Tag>();
    }
}
