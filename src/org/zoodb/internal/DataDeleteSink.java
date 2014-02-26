/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
 * 
 * This file is part of ZooDB.
 * 
 * ZooDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ZooDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ZooDB.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * See the README and COPYING files for further information. 
 */
package org.zoodb.internal;

import org.zoodb.api.impl.ZooPCImpl;

/**
 * A data sink serializes objects of a given class. It can be backed either by a file- or
 * in-memory-storage, or in future by a network channel through which data is sent to a server.
 * 
 * Each sink handles objects of one class only. Therefore sinks can be associated with 
 * ZooClassDefs and PCContext instances.
 * 
 * @author ztilmann
 */
public interface DataDeleteSink {

    public abstract void delete(ZooPCImpl obj);

    public abstract void deleteGeneric(GenericObject obj);

    /**
     * To be called after a series of write calls and before commit. Flushes the sink.
     */
    public abstract void flush();

	public abstract void reset();

}