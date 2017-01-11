/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.starter.tools;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mortbay.log.Log;

/**
 * 
 * @author hankwing
 * 
 * @param <T>
 */
public final class AvgSpeedSlotBasedCounter<T> implements Serializable {

	private static final long serialVersionUID = 4858185737378394432L;

	private final Map<T, List<long[]>> objToCounts = new HashMap<T, List<long[]>>();
	private final int numSlots;

	public AvgSpeedSlotBasedCounter(int numSlots) {
		if (numSlots <= 0) {
			throw new IllegalArgumentException(
					"Number of slots must be greater than zero (you requested "
							+ numSlots + ")");
		}
		this.numSlots = numSlots;
	}

	public void incrementCount(T obj, int slot, Integer speed) {
		List<long[]> countsList = objToCounts.get(obj);
		if( countsList == null) {
			countsList = new ArrayList<long[]>();
			countsList.add(0,new long[this.numSlots]);
			countsList.add(1,new long[this.numSlots]);
			objToCounts.put(obj, countsList);
		}
		
		long[] carCounts = objToCounts.get(obj).get(0);
		long[] speedTotal = objToCounts.get(obj).get(1);
		carCounts[slot] ++;
		speedTotal[slot] += speed;
		
	}

//	public long getCount(T obj, int slot) {
//		long[] counts = objToCounts.get(obj);
//		if (counts == null) {
//			return 0;
//		} else {
//			return counts[slot];
//		}
//	}

	public Map<T, Long> getAvgSpeed() {
		Map<T, Long> result = new HashMap<T, Long>();
		for (T obj : objToCounts.keySet()) {
			result.put(obj, computeAvgSpeed(obj));
		}
		return result;
	}

	private long computeAvgSpeed(T obj) {
		long[] curr = objToCounts.get(obj).get(0);
		long[] totalspeed = objToCounts.get(obj).get(1);
		
		long countSum = 0;
		long speedSum = 0;
		for (int i = 0; i < curr.length; i++) {
			countSum += curr[i];
			speedSum += totalspeed[i];
		}

		return speedSum/countSum;
	}
	
	private long computeTotalCount(T obj) {
	    long[] curr = objToCounts.get(obj).get(0);
	    long total = 0;
	    for (long l : curr) {
	      total += l;
	    }
	    return total;
	  }

	/**
	 * Reset the slot count of any tracked objects to zero for the given slot.
	 * 
	 * @param slot
	 */
	public void wipeSlot(int slot) {
		for (T obj : objToCounts.keySet()) {
			resetSlotCountToZero(obj, slot);
		}
	}

	private void resetSlotCountToZero(T obj, int slot) {
		long[] counts = objToCounts.get(obj).get(0);
		long[] speeds = objToCounts.get(obj).get(1);
		speeds[slot] = 0;
		counts[slot] = 0;
	}

	private boolean shouldBeRemovedFromCounter(T obj) {
		return computeTotalCount(obj) == 0;
	}

	/**
	 * Remove any object from the counter whose total count is zero (to free up
	 * memory).
	 */
	public void wipeZeros() {
		Set<T> objToBeRemoved = new HashSet<T>();
		for (T obj : objToCounts.keySet()) {
			if (shouldBeRemovedFromCounter(obj)) {
				objToBeRemoved.add(obj);
			}
		}
		for (T obj : objToBeRemoved) {
			objToCounts.remove(obj);
		}
	}

}
