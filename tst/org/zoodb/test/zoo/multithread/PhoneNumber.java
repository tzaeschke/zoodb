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
package org.zoodb.test.zoo.multithread;

import org.zoodb.api.impl.ZooPC;

/**
 * Phone number of a person.
 */
public class PhoneNumber extends ZooPC {

    long id; // PK when using app id
    String name;
    String number;

    public PhoneNumber() { }

    public PhoneNumber(String name, String number)
    {
        this.name = name;
        this.number = number;
    }

    public void setId(long id)
    {
        this.id = id;
    }

    public long getId()
    {
        return id;
    }

    public String getName()
    {
        return name;
    }

    public String getNumber()
    {
        return number;
    }
}