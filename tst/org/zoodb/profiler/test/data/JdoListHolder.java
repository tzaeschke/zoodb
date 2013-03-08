/* Copyright (C) 2004 - 2006  db4objects Inc.  http://www.db4o.com */

package org.zoodb.profiler.test.data;

import java.util.ArrayList;
import java.util.List;

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.profiler.test.data.CheckSummable;
import org.zoodb.profiler.test.data.JdoListHolder;

/**
 * @author Christian Ernst
 */
public class JdoListHolder extends PersistenceCapableImpl implements CheckSummable{
    
    public List<Integer> list;
    
    public JdoListHolder(){
        
    }
    
    public static JdoListHolder generate(int index, int elements){
        JdoListHolder lh = new JdoListHolder();
        List<Integer> list = new ArrayList<Integer>(); 
        lh.setList(list);
        for (int i = 0; i < elements; i++) {
            list.add(new Integer(i));
        }
        return lh;
    }


    public long checkSum() {
        activateRead("list");
        return list.size();
    }

    public List<Integer> getList() {
        activateRead("list");
        return list;
    }
    
    public void setList(List<Integer> list) {
    	activateWrite("list");
        this.list = list;
    }

}
