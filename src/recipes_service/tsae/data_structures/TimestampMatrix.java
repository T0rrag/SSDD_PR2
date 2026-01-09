/*
* Copyright (c) Joan-Manuel Marques 2013. All rights reserved.
* DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
*
* This file is part of the practical assignment of Distributed Systems course.
*
* This code is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This code is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this code.  If not, see <http://www.gnu.org/licenses/>.
*/

package recipes_service.tsae.data_structures;

import java.io.Serializable;
import java.util.Enumeration;
import java.util.List;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Vector;

import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;

/**
 * @author Joan-Manuel Marques, Daniel Lázaro Iglesias
 * December 2012
 *
 */
public class TimestampMatrix implements Serializable{
	
	private static final long serialVersionUID = 3331148113387926667L;
        ConcurrentHashMap<String, TimestampVector> timestampMatrix = new ConcurrentHashMap<String, TimestampVector>();
        private final List<String> participants;

        public TimestampMatrix(List<String> participants){
                // create and empty TimestampMatrix
                this.participants = new Vector<String>();
                for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
                        String id = it.next();
                        this.participants.add(id);
                        timestampMatrix.put(id, new TimestampVector(participants));
                }
        }
	
	/**
	 * @param node
	 * @return the timestamp vector of node in this timestamp matrix
	 */
        public synchronized TimestampVector getTimestampVector(String node){
        	return timestampMatrix.get(node);
        }
	
	/**
	 * Merges two timestamp matrix taking the elementwise maximum
	 * @param tsMatrix
	 */
        public synchronized void updateMax(TimestampMatrix tsMatrix){
    		if (tsMatrix == null) {
    			return;
    		}
    		for (String node : tsMatrix.timestampMatrix.keySet()) {
    			TimestampVector other = tsMatrix.timestampMatrix.get(node);
    			TimestampVector current = timestampMatrix.get(node);
    			if (current == null) {
    				timestampMatrix.put(node, other.clone());
    			} else {
    				current.updateMax(other);
    			}
    		}
        }
	
	/**
	 * substitutes current timestamp vector of node for tsVector
	 * @param node
	 * @param tsVector
	 */
        public synchronized void update(String node, TimestampVector tsVector){
    		if (node == null || tsVector == null) {
    			return;
    		}
    		timestampMatrix.put(node, tsVector.clone());
		}
	/**
	 * 
	 * @return a timestamp vector containing, for each node, 
	 * the timestamp known by all participants
	 */
	public synchronized TimestampVector minTimestampVector(){
		TimestampVector minVector = new TimestampVector(new Vector<String>(participants));
		boolean first = true;
		for (String node : participants){
			TimestampVector row = timestampMatrix.get(node);
			if (row == null){
				continue;
			}
			if (first){
				minVector = row.clone();
				first = false;
			}else{
				minVector.mergeMin(row);
			}
		}
		return minVector;
	}
	
	/**
	 * clone
	 */
        public synchronized TimestampMatrix clone(){
    		TimestampMatrix copy = new TimestampMatrix(new Vector<String>(participants));
    		for (String node : participants) {
    			TimestampVector vector = timestampMatrix.get(node);
    			if (vector != null) {
    				copy.timestampMatrix.put(node, vector.clone());
    			}
    		}
    		return copy;
        }
	
	/**
	 * equals
	 */
	@Override
        public synchronized boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!(obj instanceof TimestampMatrix)) {
			return false;
		}
		TimestampMatrix other = (TimestampMatrix) obj;
		if (!timestampMatrix.keySet().equals(other.timestampMatrix.keySet())) {
			return false;
		}
		for (String node : timestampMatrix.keySet()) {
			TimestampVector local = timestampMatrix.get(node);
			TimestampVector remote = other.timestampMatrix.get(node);
			if (local == null) {
				if (remote != null) {
					return false;
				}
			} else if (!local.equals(remote)) {
				return false;
			}
		}
		return true;
	}
	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String all="";
		if(timestampMatrix==null){
			return all;
		}
		for(Enumeration<String> en=timestampMatrix.keys(); en.hasMoreElements();){
			String name=en.nextElement();
			if(timestampMatrix.get(name)!=null)
				all+=name+":   "+timestampMatrix.get(name)+"\n";
		}
		return all;
	}
}
