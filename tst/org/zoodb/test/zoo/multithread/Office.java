/**********************************************************************
Copyright (c) 2005 Andy Jefferson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


Contributors :
 ...
***********************************************************************/
package org.zoodb.test.zoo.multithread;

import java.io.Serializable;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.zoodb.api.impl.ZooPC;

/**
 * An office in the company.
 */
public class Office extends ZooPC implements Cloneable, Serializable
{
    private static final long serialVersionUID = -463151609436855378L;
    private long floor; // PK when app-id
    private String roomName; // PK when app-id

    private String description;
    private Set<Department> departments = new HashSet<>(); // 1-N uni relation using join table
    private Date date;

    public Office()
    {
    }

    public Office(long floor, String roomName, String description)
    {
        this.floor = floor;
        this.roomName = roomName;
        this.description = description;
    }

    public Date getDate()
    {
        return date;
    }

    public void setDate(Date date)
    {
        this.date = date;
    }

    /**
     * Accessor for the roomt name
     * @return Returns the room name.
     */
    public String getRoomName()
    {
        return roomName;
    }

    /**
     * Mutator for the room name
     * @param roomName The room name
     */
    public void setRoomName(String roomName)
    {
        this.roomName = roomName;
    }

    public long getFloor()
    {
        return floor;
    }

    public void setFloor(long floor)
    {
        this.floor = floor;
    }

    public String getDescription()
    {
        return description;
    }

    public void setDescription(String s)
    {
        description = s;
    }

    public void addDepartment(Department dept)
    {
        departments.add(dept);
    }

    public void clearDepartments()
    {
        departments.clear();
    }

    public Set<Department> getDepartments()
    {
        return departments;
    }

    // Note that this is only really correct for application identity, but we also use this class for datastore id
    @Override
    public int hashCode()
    {
        zooActivateRead();
        int hash = 7;
        hash = 31 * hash + (int)floor;
        hash = 31 * hash + (null == roomName ? 0 : roomName.hashCode());
        return hash;
    }

    // Note that this is only really correct for application identity, but we also use this class for datastore id
    @Override
    public boolean equals(Object o)
    {
        if (o == this)
        {
            return true;
        }
        if ((o == null) || (o.getClass() != this.getClass()))
            return false;

        zooActivateRead();
        Office other = (Office)o;
        return floor == other.floor &&
            (roomName == other.roomName || (roomName != null && roomName.equals(other.roomName)));
    }

    public String asString()
    {
        return "Office : floor=" + getFloor() + ", room=" + getRoomName() + "- " + getDescription();
    }

    public static class Id implements Serializable
    {
        private static final long serialVersionUID = 2196040222596916094L;
        public long floor;
        public String roomName;

        public Id ()
        {
        }

        /**
         *  String constructor.
         */
        public Id (String str)
        {
            StringTokenizer toke = new StringTokenizer (str, "::");

            str = toke.nextToken ();
            this.floor = Integer.parseInt (str);
            str = toke.nextToken ();
            this.roomName = str;
        }

        /**
         *  Implementation of equals method.
         */
        @Override
        public boolean equals (Object obj)
        {
            if (obj == this)
            {
                return true;
            }

            if (!(obj instanceof Id))
            {
                return false;
            }

            Id c = (Id)obj;

            return floor == c.floor && roomName.equals(c.roomName);
        }

        /**
         *  Implementation of hashCode method that supports the
         *  equals-hashCode contract.
         */
        @Override
        public int hashCode()
        {
            return ((int)this.floor) ^ this.roomName.hashCode();
        }

        /**
         *  Implementation of toString that outputs this object id's
         *  primary key values.
         */
        @Override
        public String toString()
        {
            return String.valueOf (this.floor)
            + "::" + String.valueOf (this.roomName);
        }
    }
}