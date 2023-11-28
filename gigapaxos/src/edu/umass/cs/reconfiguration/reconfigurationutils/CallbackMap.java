/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.reconfiguration.reconfigurationutils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import edu.umass.cs.reconfiguration.reconfigurationpackets.StopEpoch;

/**
 * @author V. Arun
 * @param <NodeIDType>
 * 
 *            FIXME: May need to add a self-destruct property to entries so that
 *            they are automatically removed after some idle time.
 */
public class CallbackMap<NodeIDType> {
	private final ConcurrentHashMap<String, List<StopEpoch<NodeIDType>>> listMap = new ConcurrentHashMap<String, List<StopEpoch<NodeIDType>>>();

	/**
	 * @param stopEpoch
	 * @return True as specified by {@link List#add(Object)}.
	 */
	public synchronized boolean addStopNotifiee(StopEpoch<NodeIDType> stopEpoch) {
		//if (!this.listMap.containsKey(stopEpoch.getServiceName())) {
			//ArrayList<StopEpoch<NodeIDType>> notifiees = new ArrayList<StopEpoch<NodeIDType>>();
			//this.listMap.put(stopEpoch.getServiceName(), notifiees);
		//}
		this.listMap.putIfAbsent(stopEpoch.getServiceName(), new ArrayList<StopEpoch<NodeIDType>>());
		List<StopEpoch<NodeIDType>> notifiees = null;
		if((notifiees = this.listMap.get(stopEpoch.getServiceName())) != null)
			return notifiees.add(stopEpoch);
		//return this.listMap.get(stopEpoch.getServiceName()).add(stopEpoch);
		else return false;
	}

	/**
	 * @param name
	 * @param epoch
	 * @return StopEpoch requiring acknowledgment.
	 */
	public synchronized StopEpoch<NodeIDType> notifyStop(String name, int epoch) {
		if (!this.listMap.containsKey(name))
			return null;
		StopEpoch<NodeIDType> notifiee = null, retval = null;
		for (Iterator<StopEpoch<NodeIDType>> notifieeIter = this.listMap.get(
				name).iterator(); notifieeIter.hasNext();) {
			notifiee = notifieeIter.next();
			if (notifiee.getEpochNumber() - epoch > 0)
				continue;
			// else
			retval = notifiee;
			notifieeIter.remove();
			break;
		}
		if (this.listMap.get(name).isEmpty())
			this.listMap.remove(name);
		return retval;
	}
}
