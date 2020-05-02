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
package org.zoodb.tools;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.GenericObject;
import org.zoodb.internal.Session;
import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.ZooClassProxy;
import org.zoodb.internal.ZooFieldProxy;
import org.zoodb.internal.ZooHandleImpl;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.jdo.impl.PersistenceManagerImpl;
import org.zoodb.schema.ZooClass;
import org.zoodb.schema.ZooField;
import org.zoodb.schema.ZooHandle;
import org.zoodb.tools.internal.DataSerializer;
import org.zoodb.tools.internal.ObjectCache;
import org.zoodb.tools.internal.XmlWriter;

/**
 * Export a database to xml.
 * 
 * @author ztilmann
 *
 */
public class ZooXmlExport {

    private final Writer out;
    
    public ZooXmlExport(Writer out) {
        this.out = out;
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Error: invalid number of arguments.");
            System.out.println("Usage: ");
            System.out.println("    XmlExport <dbName> <xmlFileName>");
            return;
        }
        
        String dbName = args[0];
        String xmlName = args[1];
        Writer out = openFile(xmlName);
        if (out == null) {
            return;
        }
        
        try {
            new ZooXmlExport(out).writeDB(dbName);
            out.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    public void writeDB(String dbName) {
        ZooJdoProperties props = new ZooJdoProperties(dbName);
        PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);
        PersistenceManager pm = pmf.getPersistenceManager();
        try {
        	pm.currentTransaction().begin();
        	writeToXML(pm);
        } finally {
            pm.currentTransaction().rollback();
            pm.close();
            pmf.close();
        }
    }
    
    
    private void writeToXML(PersistenceManager pm) {
		Session session = ((PersistenceManagerImpl)pm).getSession();
    	ObjectCache cache = new ObjectCache(session);

    	writeln("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>");
        writeln("<database>");
        
        writeln(" <schema>");
        for (ZooClass sch: ZooJdoHelper.schema(pm).getAllClasses()) {
            if (sch.getJavaClass() == ZooPC.class) {
                continue;
            }
            ZooClassProxy prx = (ZooClassProxy) sch;
            cache.addSchema(prx.getSchemaDef().getOid(), prx.getSchemaDef());
            writeln("  <class " +
                    "name=\"" + sch.getName() + 
                    "\" oid=\"" + prx.getSchemaDef().getOid() + 
                    "\" super=\"" + prx.getSchemaDef().getSuperOID() + 
                    "\">");
            for (ZooField f: sch.getAllFields()) {
            	writeln("   <attr id=\"" + ((ZooFieldProxy)f).getFieldDef().getFieldPos() + 
            			"\" name=\"" + f.getName() + 
            			"\" type=\"" + f.getTypeName() +
            			"\" arrayDim=\"" + f.getArrayDim() + "\" />");
            }
            writeln("  </class>");
        }
        writeln(" </schema>");
        
        writeln(" <data>");
        for (ZooClass sch: ZooJdoHelper.schema(pm).getAllClasses()) {
        	if (ZooClassDef.class.isAssignableFrom(sch.getJavaClass())) {
        		continue;
        	}
            ZooClassDef def = ((ZooClassProxy) sch).getSchemaDef();
            writeln("  <class oid=\"" + def.getOid() + "\" name=\"" + sch.getName() + "\">");
            Iterator<ZooHandle> it = sch.getHandleIterator(false);
        	XmlWriter w = new XmlWriter(out);
        	DataSerializer ser = new DataSerializer(w, cache);
            while (it.hasNext()) {
            	ZooHandle hdl = it.next();
            	GenericObject go = ((ZooHandleImpl)hdl).getGenericObject(); 
            	ser.writeObject(go, def);
            }
            writeln("  </class>");
        }
        writeln(" </data>");
        
        writeln("</database>");
    }

    private static Writer openFile(String xmlName) {
        File file = new File(xmlName);
        if (file.exists()) {
            System.out.println("File already exists: " + file);
            return null;
        }
        try {
            if (!file.createNewFile()) {
                System.out.println("Could not create file: " + file);
                return null;
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Could not create file: " + file);
        }
        
        try {
            FileOutputStream fos = new FileOutputStream(file);
            return new OutputStreamWriter(fos, StandardCharsets.UTF_8);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
    
    private void writeln(String str) {
        try {
            out.append(str);
            out.append('\n');
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
}
