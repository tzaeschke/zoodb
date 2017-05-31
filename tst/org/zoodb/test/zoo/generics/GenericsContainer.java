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

import java.util.HashMap;
import java.util.HashSet;

import org.zoodb.api.impl.ZooPC;

/**
 * Sample class that has collections/maps defined using JDK1.5 generics and so is a test for
 * the lack of specification of JDO &lt;collection&gt;, &lt;map&gt;
 * @version $Revision: 1.1 $
 */
public class GenericsContainer extends ZooPC
{
    long id;
    String name;
    HashSet<GenericsElement> elements = new HashSet<GenericsElement>();
    HashMap<String, GenericsValue> valueMap = new HashMap<String, GenericsValue>();

    public GenericsContainer() { }

    public GenericsContainer(String name)
    {
        this.name = name;
    }

    public void setId(long id)
    {
        this.id = id;
    }

    public void addElement(GenericsElement element)
    {
        elements.add(element);
    }

    public void addEntry(String key, GenericsValue value)
    {
        valueMap.put(key, value);
    }

    public HashSet<GenericsElement> getElements()
    {
        return elements;
    }

    public HashMap<String, GenericsValue> getValueMap()
    {
        return valueMap;
    }
}