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

import org.zoodb.api.impl.ZooPC;

/**
 * Element stored in a JDK1.5 generics collection
 * @version $Revision: 1.1 $
 */
public class GenericsElement extends ZooPC
{
    long id;
    String name;

    public GenericsElement() { }

    /**
     * Constructor.
     * @param name Name of the element
     */
    public GenericsElement(String name)
    {
        this.name = name;
    }
}