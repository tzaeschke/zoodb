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


Contributors:
    ...
**********************************************************************/
package org.zoodb.test.zoo.multithread;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Manager of a set of Employees, and departments.
 */
public class Manager extends Employee implements Serializable
{
    private static final long serialVersionUID = -1710801247113719913L;
    protected Set<Employee> subordinates;
    protected Set<Department> departments;

    protected Manager()
    {
    }

    public Manager(long id, String firstname, String lastname, String email, float salary, String serial)
    {
        super(id, firstname, lastname, email, salary, serial);
        this.departments = new HashSet<>();
        this.subordinates = new HashSet<>();
    }

    public Set<Employee> getSubordinates()
    {
        return this.subordinates;
    }

    public void addSubordinate(Employee e)
    {
        this.subordinates.add(e);
    }

    public void removeSubordinate(Employee e)
    {
        this.subordinates.remove(e);
    }

    public void addSubordinates(Collection<? extends Employee> c)
    {
        this.subordinates.addAll(c);
    }

    public void clearSubordinates()
    {
        this.subordinates.clear();
    }

    public Set<Department> getDepartments()
    {
        return this.departments;
    }

    public void addDepartment(Department d)
    {
        this.departments.add(d);
    }

    public void removeDepartment(Department d)
    {
        this.departments.remove(d);
    }

    public void clearDepartments()
    {
        this.departments.clear();
    }

	/**
     * Compares two sets of Person. Returns true if and only if the two sets
     * contain the same number of objects and each element of the first set has
     * a corresponding element in the second set whose fields compare equal
     * according to the compareTo() method.
     * @return <tt>true</tt> if the sets compare equal, <tt>false</tt>
     * otherwise.
     */
    public static boolean compareSet(Set s1, Set<? extends Object> s2)
    {
        if (s1 == null)
        {
            return s2 == null;
        }
        else if (s2 == null)
        {
            return false;
        }

        if (s1.size() != s2.size())
        {
            return false;
        }

        s2 = new HashSet<>(s2);
        Iterator i = s1.iterator();
        while (i.hasNext())
        {
            Person obj = (Person) i.next();

            boolean found = false;
            Iterator j = s2.iterator();
            while (j.hasNext())
            {
                if (obj.compareTo(j.next()))
                {
                    j.remove();
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                return false;
            }
        }

        return true;
    }

    /**
     * Compares two sets of Person. Returns true if and only if the two sets
     * contain the same number of objects and each element of the first set has
     * a corresponding element in the second set whose fields compare equal
     * according to the compareTo() method.
     * @return <tt>true</tt> if the sets compare equal, <tt>false</tt> otherwise.
     */
    public static boolean compareElementsContained(Set s1, Set<? extends Object> s2)
    {
        if (s1 == null)
        {
            return s2 == null;
        }
        else if (s2 == null)
        {
            return false;
        }

        if (s1.size() != s2.size())
        {
            return false;
        }

        s2 = new HashSet<>(s2);
        Iterator i = s1.iterator();
        while (i.hasNext())
        {
            Person p1 = (Person) i.next();

            boolean found = false;
            Iterator j = s2.iterator();
            while (j.hasNext())
            {
                Person p2 = (Person)j.next();
                if (p1.getFirstName().equals(p2.getFirstName()) && p1.getLastName().equals(p2.getLastName()))
                {
                    j.remove();
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                return false;
            }
        }

        return true;
    }
}