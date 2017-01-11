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

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.tuple.Tuple;

/**
 * This class provides per-slot counts of the occurrences of objects.
 * <p/>
 * It can be used, for instance, as a building block for implementing sliding
 * window counting of objects.
 *
 * @param <T>
 *            The type of those objects we want to count.
 */
public class JoinBasedCounter<T> implements Serializable {

	private static final long serialVersionUID = 4858185737378394432L;

	private Map<T, Map<GlobalStreamId, List<Tuple>>[]> objToCounts = null;
	
	private int numSlots;

	public JoinBasedCounter(int numSlots) {
		if (numSlots <= 0) {
			throw new IllegalArgumentException(
					"Number of slots must be greater than zero (you requested "
							+ numSlots + ")");
		}
		objToCounts = new HashMap<T, Map<GlobalStreamId, List<Tuple>>[]>();
		this.numSlots = numSlots;
	}

	/**
	 * 
	 * @param obj
	 *            join key
	 * @param slot
	 * @param streamId
	 * @param tuple
	 */
	public void addToSlot(T obj, int slot, GlobalStreamId streamId, Tuple tuple) {
		Map<GlobalStreamId, List<Tuple>>[] counts = objToCounts.get(obj);
		if (counts == null) {
			counts = new HashMap[this.numSlots];
			for (int i = 0; i < this.numSlots; i++) {
				counts[i] = new HashMap<GlobalStreamId, List<Tuple>>();
			}

			List<Tuple> tuples = new ArrayList<Tuple>();
			tuples.add(tuple);
			counts[slot].put(streamId, tuples);
			objToCounts.put(obj, counts);
		} else if (counts[slot].get(streamId) == null) {
			List<Tuple> tuples = new ArrayList<Tuple>();
			tuples.add(tuple);
			counts[slot].put(streamId, tuples);
		} else {
			counts[slot].get(streamId).add(tuple);
		}
		
	}

	/*
	 * public long getCount(T obj, int slot) { long[] counts =
	 * objToCounts.get(obj); if (counts == null) { return 0; } else { return
	 * counts[slot]; } }
	 */

	/*
	 * public Map<T, Long> getCounts() { Map<T, Long> result = new HashMap<T,
	 * Long>(); for (T obj : objToCounts.keySet()) { result.put(obj,
	 * computeTotalCount(obj)); } return result; }
	 * 
	 * private long computeTotalCount(T obj) { long[] curr =
	 * objToCounts.get(obj); long total = 0; for (long l : curr) { total += l; }
	 * return total; }
	 */

	/**
	 * Reset the slot count of any tracked objects to zero for the given slot.
	 *
	 * @param slot
	 */
	public Map<T, Map<GlobalStreamId, List<Tuple>>> getRemoveTuplesAndWipeSlot(
			int slot) {
		Map<T, Map<GlobalStreamId, List<Tuple>>> removeTuples = new HashMap<T, Map<GlobalStreamId, List<Tuple>>>();
		for (T obj : objToCounts.keySet()) {
			if( !objToCounts.get(obj)[slot].isEmpty()) {
				removeTuples.put(obj, objToCounts.get(obj)[slot]);
			}
			// resetSlot(obj, slot);
		}
		return removeTuples;
	}

	/**
	 * reset slot and remove empty keys
	 * 
	 * @param slot
	 */
	public void resetSlot(int slot) {

		Set<T> objToBeRemoved = new HashSet<T>();
		for (T obj : objToCounts.keySet()) {

			objToCounts.get(obj)[slot].clear();

			if (shouldBeRemovedFromCounter(obj)) {
				objToBeRemoved.add(obj);
			}
		}
		for (T obj : objToBeRemoved) {
			objToCounts.remove(obj);
		}

	}

	private boolean shouldBeRemovedFromCounter(T obj) {

		boolean shoudBeRemoved = true;
		for (int i = 0; i < numSlots; i++) {
			if (!objToCounts.get(obj)[i].isEmpty()) {
				shoudBeRemoved = false;
				break;
			}
		}
		return shoudBeRemoved;
	}

}
