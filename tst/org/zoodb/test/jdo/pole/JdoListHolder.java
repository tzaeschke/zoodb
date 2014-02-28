/* Copyright (C) 2004 - 2006  db4objects Inc.  http://www.db4o.com */

package org.zoodb.test.jdo.pole;

import java.util.ArrayList;
import java.util.List;

import org.zoodb.api.impl.ZooPCImpl;

/**
 * @author Christian Ernst
 */
public class JdoListHolder extends ZooPCImpl implements CheckSummable{
    
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
        zooActivateRead();
        return list.size();
    }

    public List<Integer> getList() {
        zooActivateRead();
        return list;
    }
    
    public void setList(List<Integer> list) {
    	zooActivateWrite();
        this.list = list;
    }

}
