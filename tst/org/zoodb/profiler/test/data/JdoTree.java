/* 
This file is part of the PolePosition database benchmark
http://www.polepos.org

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public
License along with this program; if not, write to the Free
Software Foundation, Inc., 59 Temple Place - Suite 330, Boston,
MA  02111-1307, USA. */

package org.zoodb.profiler.test.data;

import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.profiler.test.data.JdoTree;
import org.zoodb.profiler.test.data.JdoTreeVisitor;


public class JdoTree extends PersistenceCapableImpl {
    
    private static long idGenerator;
    
    public long id;
    public JdoTree preceding;
    public JdoTree subsequent;
    public String name;
    public int depth;
    
    public JdoTree(){
    }
    
    public JdoTree(long id, String name, int depth){
        this.id = id;
        this.name = name;
        this.depth = depth;
    }
    
    public static JdoTree createTree(int depth){
        idGenerator = 0;
        return createTree(depth, 0);
    }
    
    private static JdoTree createTree(int maxDepth, int currentDepth){
        
        if(maxDepth <= 0){
            return null;
        }
        
        JdoTree tree = new JdoTree();
        if(currentDepth == 0){
            tree.name = "root";
        }else{
            tree.name = "node at depth " + currentDepth;
        }
        tree.id = ++idGenerator;
        tree.depth =  currentDepth;
        tree.preceding = createTree(maxDepth - 1, currentDepth + 1); 
        tree.subsequent = createTree(maxDepth - 1, currentDepth + 1); 
        return tree;
    }
    
    public static void traverse(JdoTree tree, JdoTreeVisitor visitor){
        if(tree == null){
            return;
        }
        

        traverse(tree.preceding, visitor);
        traverse(tree.subsequent, visitor);
        visitor.visit(tree);
    }
    
    public int getDepth(){
    	activateRead("depth");
        return depth;
    }
    
    

}
