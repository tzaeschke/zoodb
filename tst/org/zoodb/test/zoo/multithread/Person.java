/**********************************************************************
Copyright (c) 2003 Mike Martin (TJDO) and others. All rights reserved.
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
2008 Andy Jefferson - fixed hashCode, equals to remove JDO dependency that failed outside of persistence contexts
    ...
***********************************************************************/
package org.zoodb.test.zoo.multithread;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;

import org.zoodb.api.impl.ZooPC;

/**
 * Person in a company.
 */
public class Person extends ZooPC implements Cloneable, Serializable {

    private static final long serialVersionUID = 4488301280191662133L;
    private long personNum; // Part of PK when app id
    private String globalNum; // Part of PK when app id

    private String firstName;
    private String lastName;
    private String emailAddress;
    private int age;
    private Date birthDate;

    private Person bestFriend;

    private Map<String, PhoneNumber> phoneNumbers = new HashMap<>();

    /** Used for the querying of static fields. */
    public static final String FIRSTNAME="Woody";

    public Person()
    {
    }

    public Person(long num, String first, String last, String email)
    {
        globalNum = "global:" +Math.abs(new Random().nextInt());
        personNum = num;
        firstName = first;
        lastName = last;
        emailAddress = email;
    }

    public void setBirthDate(Date birthDate)
    {
        this.birthDate = birthDate;
    }

    public Date getBirthDate()
    {
        return birthDate;
    }

    public void setBestFriend(Person p)
    {
        this.bestFriend = p;
    }

    public Person getBestFriend()
    {
        return bestFriend;
    }

    public Map<String, PhoneNumber> getPhoneNumbers()
    {
        return phoneNumbers;
    }

    public String getGlobalNum()
    {
        return globalNum;
    }

    public void setGlobalNum(String globalNum)
    {
        this.globalNum = globalNum;
    }

    public int getAge()
    {
        return age;
    }

    public void setAge(int age)
    {
        this.age = age;
    }

    @Override
    public Object clone()
    {
        Object o = null;

        try
        {
            o = super.clone();
        }
        catch (CloneNotSupportedException e)
        {
            /* can't happen */
        }

        return o;
    }

    public long getPersonNum()
    {
        return personNum;
    }

    public void setPersonNum(long num)
    {
        personNum = num;
    }

    public String getFirstName()
    {
        return firstName;
    }

    public void setFirstName(String s)
    {
        firstName = s;
    }

    public synchronized String getLastName()
    {
        return lastName;
    }

    public void setLastName(String s)
    {
        lastName = s;
    }

    public String getEmailAddress()
    {
        return emailAddress;
    }

    public void setEmailAddress(String s)
    {
        emailAddress = s;
    }

	public boolean compareTo(Object obj)
	{
        // TODO Use globalNum here too ?
		Person p = (Person)obj;
		return bestFriend == p.bestFriend &&
            firstName.equals(p.firstName) &&
            lastName.equals(p.lastName) &&
            emailAddress.equals(p.emailAddress) &&
            personNum == p.personNum;
	}

	// Note that this is only really correct for application identity, but we also use this class for datastore id
	@Override
    public int hashCode()
    {
	    zooActivateRead();
	    int hash = 7;
	    hash = 31 * hash + (int)personNum;
	    hash = 31 * hash + (null == globalNum ? 0 : globalNum.hashCode());
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
        Person other = (Person)o;
        return personNum == other.personNum &&
            (globalNum == other.globalNum || (globalNum != null && globalNum.equals(other.globalNum)));
    }

    public String asString()
    {
        return "Person : number=" + getPersonNum() +
            " forename=" + getFirstName() + " surname=" + getLastName() +
            " email=" + getEmailAddress() + " bestfriend=" + getBestFriend();
    }

    public static class Id implements Serializable
    {
        private static final long serialVersionUID = -4927244110186988295L;
        public long personNum;
        public String globalNum;

        public Id ()
        {
        }

        public Id (String str)
        {
            StringTokenizer toke = new StringTokenizer (str, "::");

            str = toke.nextToken ();
            this.personNum = Integer.parseInt (str);
            str = toke.nextToken ();
            this.globalNum = str;
        }

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
            return personNum == c.personNum && globalNum.equals(c.globalNum);
        }

        @Override
        public int hashCode()
        {
            return ((int)this.personNum) ^ this.globalNum.hashCode();
        }

        @Override
        public String toString()
        {
            return String.valueOf (this.personNum) + "::" + String.valueOf (this.globalNum);
        }
    }
}