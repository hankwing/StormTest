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
import java.util.Map;

/**
 * 
 * @author hankwing
 * 
 * @param <T>
 */
public final class AvgSpeedSlidingWindowCounter<T> implements Serializable {

	private static final long serialVersionUID = -2645063988768785810L;

	private AvgSpeedSlotBasedCounter<T> objCounter;
	private int headSlot;
	private int tailSlot;

	private int windowLengthInSlots;

	public AvgSpeedSlidingWindowCounter(int windowLengthInSlots) {
		if (windowLengthInSlots < 2) {
			throw new IllegalArgumentException(
					"Window length in slots must be at least two (you requested "
							+ windowLengthInSlots + ")");
		}
		this.windowLengthInSlots = windowLengthInSlots;
		this.objCounter = new AvgSpeedSlotBasedCounter<T>(this.windowLengthInSlots);
		this.headSlot = 0;
		this.tailSlot = slotAfter(headSlot);
	}

	public void incrementCount(T obj, Integer speed) {
		objCounter.incrementCount(obj, headSlot, speed);
	}

	/**
	 * Return the current (total) counts of all tracked objects, then advance
	 * the window.
	 * <p/>
	 * Whenever this method is called, we consider the counts of the current
	 * sliding window to be available to and successfully processed "upstream"
	 * (i.e. by the caller). Knowing this we will start counting any subsequent
	 * objects within the next "chunk" of the sliding window.
	 * 
	 * @return The current (total) counts of all tracked objects.
	 */
	public Map<T, Long> getAvgsThenAdvanceWindow() {
		Map<T, Long> counts = objCounter.getAvgSpeed();
		
		objCounter.wipeSlot(tailSlot);
		objCounter.wipeZeros();
		advanceHead();
		return counts;
	}

	private void advanceHead() {
		headSlot = tailSlot;
		tailSlot = slotAfter(tailSlot);
	}

	private int slotAfter(int slot) {
		return (slot + 1) % windowLengthInSlots;
	}

}
