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
package storm.starter.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import storm.starter.util.TupleHelpers;

import com.esotericsoftware.minlog.Log;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.RotatingMap;

/**
 * 
 * @author hankwing
 *
 */
public class WayJoinBolt extends BaseRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	OutputCollector _collector;
	Fields _idFields;
	Fields _outFields;
	int _numSources;
	RotatingMap<List<Object>, Map<GlobalStreamId, Tuple>> _pending;	//save temp data
	Map<String, GlobalStreamId> _fieldLocations;

	public WayJoinBolt(Fields outFields) {
		_outFields = outFields;
	}

	public WayJoinBolt() {
		// TODO Auto-generated constructor stub
		_outFields = new Fields("xway","dir","seg","volume","avgspeed");
	}

	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		_fieldLocations = new HashMap<String, GlobalStreamId>();
		_collector = collector;
		_pending = new RotatingMap<List<Object>, Map<GlobalStreamId, Tuple>>(
				new ExpireCallback());
		_numSources = context.getThisSources().size();
		Log.info("source number: " + _numSources);
		Set<String> idFields = null;
		for (GlobalStreamId source : context.getThisSources().keySet()) {
			Fields fields = context.getComponentOutputFields(
					source.get_componentId(), source.get_streamId());
			Set<String> setFields = new HashSet<String>(fields.toList());
			if (idFields == null)
				idFields = setFields;
			else
				idFields.retainAll(setFields);

			for (String outfield : _outFields) {
				for (String sourcefield : fields) {
					if (outfield.equals(sourcefield)) {
						_fieldLocations.put(outfield, source);
					}
				}
			}
		}
		_idFields = new Fields(new ArrayList<String>(idFields));

		if (_fieldLocations.size() != _outFields.size()) {
			throw new RuntimeException(
					"Cannot find all outfields among sources");
		}
	}

	@Override
	public void execute(Tuple tuple) {
		if (TupleHelpers.isTickTuple(tuple)) {
		      //Log.info("Received tick tuple, triggering emit of current window counts");
		      // do something
		}
		else {
			List<Object> id = tuple.select(_idFields);
			GlobalStreamId streamId = new GlobalStreamId(
					tuple.getSourceComponent(), tuple.getSourceStreamId());
			if (!_pending.containsKey(id)) {
				_pending.put(id, new HashMap<GlobalStreamId, Tuple>());
			}
			Map<GlobalStreamId, Tuple> parts = _pending.get(id);
//			if (parts.containsKey(streamId))
//				throw new RuntimeException(
//						"Received same side of single join twice");
			parts.put(streamId, tuple);
			_collector.ack(tuple);
			if (parts.size() == _numSources) {
				_pending.remove(id);
				List<Object> joinResult = new ArrayList<Object>();
				for (String outField : _outFields) {
					GlobalStreamId loc = _fieldLocations.get(outField);
					joinResult.add(parts.get(loc).getValueByField(outField));
				}
				if ( (Long) joinResult.get(4) < 40) {

					long toll = 15 * ((long)joinResult.get(3) - 150)
							* ((long)joinResult.get(3) - 150);
					joinResult.add(toll);
					//Log.info("emit join result: " + joinResult.toString());
					_collector.emit(joinResult);
				}

//				for (Tuple part : parts.values()) {
//					_collector.ack(part);
//				}
			}
		}
		
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Fields tempFields = new Fields("xway","dir","seg","volume","avgspeed","toll");
		declarer.declare(tempFields);
		
	}

	private class ExpireCallback
			implements
			RotatingMap.ExpiredCallback<List<Object>, Map<GlobalStreamId, Tuple>> {
		@Override
		public void expire(List<Object> id, Map<GlobalStreamId, Tuple> tuples) {
			throw new RuntimeException(
					"expired data error!!!");
		}
	}
}
