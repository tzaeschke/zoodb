/**********************************************************************
Copyright (c) 2016 Andy Jefferson and others. All rights reserved.
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

import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

import org.zoodb.api.impl.ZooPC;

/**
 * Owner of GenericsBaseSub.
 */
@PersistenceCapable
public class GenericsBaseSubRelated extends ZooPC
{
    @PrimaryKey
    long id;

    @Persistent
    GenericsBaseSub baseSub;

    public GenericsBaseSubRelated() { }

    public long getId()
    {
        return id;
    }
    public void setId(long id)
    {
        this.id = id;
    }

    public GenericsBaseSub getBaseSub()
    {
        return baseSub;
    }
    public void setBaseSub(GenericsBaseSub bs)
    {
        this.baseSub = bs;
    }
}
