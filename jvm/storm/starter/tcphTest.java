package storm.starter;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import storm.starter.tools.JoinWindowCounter;
import storm.starter.util.TupleHelpers;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.RotatingMap;
import backtype.storm.utils.Utils;

import com.sun.istack.internal.logging.Logger;

/**
 * 
 * @author hankwing
 * 
 */
public class tcphTest {

	private static Logger LOG = Logger.getLogger(tcphTest.class);

	public static void main(String[] args) {

		//new TPCHProducer().produce();
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("customerspout", new CustomerSpout());
		builder.setSpout("orderspout", new OrdersSpout());
		builder.setBolt("joinbolt",
				new LineItemOrdersJoinBolt(new Fields("mktsegment", "orderkey"), 15, 5))
				.fieldsGrouping("customerspout", new Fields("custkey"))
				.fieldsGrouping("orderspout", new Fields("custkey"));
		builder.setBolt("countbolt", new CountBolt())
				.fieldsGrouping("joinbolt", new Fields("mktsegment"));

		Config conf = new Config();
		// conf.setDebug(true);
		
		if (args != null && args.length > 0) {
			
		      conf.setNumWorkers(3);
		      try {
				StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
			} catch (AlreadyAliveException | InvalidTopologyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else {
			conf.setNumWorkers(2);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(10000000);
			cluster.killTopology("test");
			cluster.shutdown();
		}

	}

	public static class CustomerSpout extends BaseRichSpout {

		SpoutOutputCollector _collector = null;
		BufferedReader br = null;

		@Override
		public void nextTuple() {
			// TODO Auto-generated method stub
			//Utils.sleep(100);
			try {
				String singleTuple = br.readLine();
				if( singleTuple != null) {
					String[] strings = singleTuple.split("\\|");
					
					_collector.emit(new Values(strings[0], strings[6]));
					Utils.sleep(1);
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		public void open(Map arg0, TopologyContext arg1,
				SpoutOutputCollector arg2) {
			// TODO Auto-generated method stub
			try {
				FileReader fr = new FileReader("tpch/0.01G/customer.tbl");
				br = new BufferedReader(fr);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			_collector = arg2;
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer arg0) {
			// TODO Auto-generated method stub
			arg0.declare(new Fields("custkey", "mktsegment"));
		}

	}

	public static class OrdersSpout extends BaseRichSpout {

		SpoutOutputCollector _collector = null;
		BufferedReader br = null;

		@Override
		public void nextTuple() {
			// TODO Auto-generated method stub
			//Utils.sleep(100);
			try {
				String singleTuple = br.readLine();
				if( singleTuple != null) {
					String[] strings = singleTuple.split("\\|");
					_collector.emit(new Values(strings[0], strings[1]));
					Utils.sleep(1);
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		public void open(Map arg0, TopologyContext arg1,
				SpoutOutputCollector arg2) {
			// TODO Auto-generated method stub
			try {
				FileReader fr = new FileReader("tpch/0.01G/orders.tbl");
				br = new BufferedReader(fr);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			_collector = arg2;
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer arg0) {
			// TODO Auto-generated method stub
			arg0.declare(new Fields("orderkey", "custkey"));
		}

	}

	/*public static class JoinBolt extends BaseRichBolt {

		OutputCollector _collector;
		Fields _joinKeyFields;
		Fields _outFields;
		int _numSources;
		RotatingMap<List<Object>, Map<GlobalStreamId, List<Tuple>>> _pending; // save
																		// temp
																		// data
		Map<String, GlobalStreamId> _fieldLocations;

		public JoinBolt(Fields outFields) {
			_outFields = outFields;
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer arg0) {
			// TODO Auto-generated method stub
			arg0.declare(_outFields);
		}

		@Override
		public void execute(Tuple tuple) {
			// TODO Auto-generated method stub

			List<Object> joinKey = tuple.select(_joinKeyFields);
			GlobalStreamId streamId = new GlobalStreamId(
					tuple.getSourceComponent(), tuple.getSourceStreamId());
			if (!_pending.containsKey(joinKey)) {
				_pending.put(joinKey, new HashMap<GlobalStreamId, List<Tuple>>());
			}
			Map<GlobalStreamId, List<Tuple>> parts = _pending.get(joinKey);
			if (parts.containsKey(streamId)) {
				
				parts.get(streamId).add(tuple);
			}
			else {
				List<Tuple> dataList = new ArrayList<Tuple>();
				dataList.add(tuple);
				parts.put(streamId, dataList);
			}
			
			if (parts.size() == _numSources) {

				while( parts.get(_fieldLocations.get("orderkey")).size() > 0) {
					
					List<Object> joinResult = new ArrayList<Object>();
					List<Tuple> anchorTuples = new ArrayList<Tuple>();
					for (String outField : _outFields) {
						GlobalStreamId loc = _fieldLocations.get(outField);
						joinResult.add(parts.get(loc).get(0).getValueByField(outField));
						anchorTuples.add(parts.get(loc).get(0));
					}
					_collector.emit(anchorTuples,
							joinResult);
					//LOG.info("join result: " + joinResult.toString());
					for (Tuple part : anchorTuples) {
						_collector.ack(part);
					}
					
					parts.get(_fieldLocations.get("orderkey")).remove(0);
					
				}
//				for (GlobalStreamId tempId : parts.keySet()) {
//
//					if (tempId.get_componentId().equals("orderspout")) {
//						parts.remove(tempId);
//						break;
//					}
//				}
				
			}
		}

		@Override
		public void prepare(Map conf, TopologyContext context,
				OutputCollector arg2) {
			// TODO Auto-generated method stub
			_fieldLocations = new HashMap<String, GlobalStreamId>();
			_collector = arg2;
			_pending = new RotatingMap<List<Object>, Map<GlobalStreamId, List<Tuple>>>(
					new ExpireCallback());
			_numSources = context.getThisSources().size();
			Set<String> joinKeyFields = null;
			for (GlobalStreamId source : context.getThisSources().keySet()) {
				Fields fields = context.getComponentOutputFields(
						source.get_componentId(), source.get_streamId());
				Set<String> setFields = new HashSet<String>(fields.toList());
				if (joinKeyFields == null)
					joinKeyFields = setFields;
				else
					joinKeyFields.retainAll(setFields);

				for (String outfield : _outFields) {
					for (String sourcefield : fields) {
						if (outfield.equals(sourcefield)) {
							_fieldLocations.put(outfield, source);
						}
					}
				}
			}
			_joinKeyFields = new Fields(new ArrayList<String>(joinKeyFields));

			if (_fieldLocations.size() != _outFields.size()) {
				throw new RuntimeException(
						"Cannot find all outfields among sources");
			}
		}

		private class ExpireCallback
				implements
				RotatingMap.ExpiredCallback<List<Object>, Map<GlobalStreamId, List<Tuple>>> {
			@Override
			public void expire(List<Object> id,
					Map<GlobalStreamId, List<Tuple>> tuples) {
				for	( List<Tuple> tupleList : tuples.values()) {
					
					for (Tuple tuple : tupleList) {
						_collector.fail(tuple);
						
					}
				}
				
			}
		}

	}*/
	
	
	/**
	 * join lineitem and orders with orderkey
	 * 
	 * @author hankwing
	 *
	 */
	public static class LineItemOrdersJoinBolt extends BaseRichBolt {

		OutputCollector _collector;
		Fields _joinKeyFields;
		Fields _outFields;
		int _numSources;
		GlobalStreamId outsideTableId = null;
		GlobalStreamId insideTableId = null;
		private final JoinWindowCounter<Object> counter;
		private final int windowLengthInSeconds;
		private final int emitFrequencyInSeconds;

		RotatingMap<List<Object>, Map<Tuple, Integer>> outsideTable; // save
																		// temp
																		// data

		RotatingMap<List<Object>, List<Tuple>> insideTable;

		Map<String, GlobalStreamId> _fieldLocations;

		public LineItemOrdersJoinBolt(Fields outFields,
				int windowLengthInSeconds, int emitFrequencyInSeconds) {
			this.windowLengthInSeconds = windowLengthInSeconds;
			this.emitFrequencyInSeconds = emitFrequencyInSeconds;
			counter = new JoinWindowCounter<Object>(
					deriveNumWindowChunksFrom(this.windowLengthInSeconds,
							this.emitFrequencyInSeconds));

			_outFields = outFields;
		}

		private int deriveNumWindowChunksFrom(int windowLengthInSeconds,
				int windowUpdateFrequencyInSeconds) {
			return windowLengthInSeconds / windowUpdateFrequencyInSeconds;
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer arg0) {
			// TODO Auto-generated method stub
			arg0.declare(_outFields);
		}

		@Override
		public void execute(Tuple tuple) {
			// TODO Auto-generated method stub
			if (TupleHelpers.isTickTuple(tuple)) {
				 LOG.info("Received tick tuple, triggering emit of current window counts");
				
				// remove expired tuple
				Map<Object, Map<GlobalStreamId, List<Tuple>>> tuplesToBeRemoved = 
						counter.getRemoveTuplesThenAdvanceWindow();
				for( Object key : tuplesToBeRemoved.keySet()) {
					for( GlobalStreamId id : tuplesToBeRemoved.get(key).keySet()) {
						if( id.equals(outsideTableId)) {
							//remove outside tuples
							for( Tuple removeTuple: tuplesToBeRemoved.get(key).get(id)) {
								outsideTable.get((List<Object>) key).remove(removeTuple);
							}
							
						}
						else {
							insideTable.get((List<Object>) key).
							removeAll(tuplesToBeRemoved.get(key).get(id));
						}
					}
				}
				
				counter.resetAndadvanceHead();	// reset slot and remove empty keys
				
			} else {

				List<Object> joinKey = tuple.select(_joinKeyFields);
				
				Map<Tuple, Integer> outsideParts = outsideTable.get(joinKey);
				List<Tuple> insideParts = insideTable.get(joinKey);
				GlobalStreamId streamId = new GlobalStreamId(
						tuple.getSourceComponent(), tuple.getSourceStreamId());
				
				counter.addToSlot(joinKey, streamId, tuple);	// add to slot
				
				if (outsideTableId.equals(streamId)) {
					// receive outside table tuple
					if (!outsideTable.containsKey(joinKey)) {
						outsideTable.put(joinKey, new HashMap<Tuple, Integer>());
						outsideParts = outsideTable.get(joinKey);
					}

					outsideParts.put(tuple, 0);
				} else {
					// receive inside table tuple
					if (!insideTable.containsKey(joinKey)) {
						insideTable.put(joinKey, new ArrayList<Tuple>());
						insideParts = insideTable.get(joinKey);
					}
					insideParts.add(tuple);
				}

				if (outsideParts != null && insideParts != null) {
					for (Entry<Tuple, Integer> tupleWithInteger : outsideParts
							.entrySet()) {

						int nowIndex = tupleWithInteger.getValue();
						while (nowIndex < insideParts.size()) {
							// join
							List<Object> joinResult = new ArrayList<Object>();
							// List<Tuple> anchorTuples = new ArrayList<Tuple>();
							for (String outField : _outFields) {
								GlobalStreamId loc = _fieldLocations.get(outField);
								if (loc.equals(outsideTableId)) {
									joinResult.add(tupleWithInteger.getKey()
											.getValueByField(outField));

								} else {
									joinResult.add(insideParts.get(nowIndex)
											.getValueByField(outField));
								}

								// anchorTuples.add(parts.get(loc).get(0));
							}
							_collector.emit(joinResult);

							// LOG.info("join result: " + joinResult.toString());
							/*
							 * for (Tuple part : anchorTuples) {
							 * _collector.ack(part); }
							 */

							nowIndex++;

						}

						tupleWithInteger.setValue(insideParts.size());
					}
				}
			}

		}

		@Override
		public void prepare(Map conf, TopologyContext context,
				OutputCollector arg2) {
			// TODO Auto-generated method stub
			_fieldLocations = new HashMap<String, GlobalStreamId>();
			_collector = arg2;
			outsideTable = new RotatingMap<List<Object>, Map<Tuple, Integer>>(
					new OutsideExpireCallback());
			insideTable = new RotatingMap<List<Object>, List<Tuple>>(
					new InsideExpireCallback());
			_numSources = context.getThisSources().size();
			Set<String> joinKeyFields = null;
			for (GlobalStreamId source : context.getThisSources().keySet()) {
				if (outsideTableId == null) {
					outsideTableId = source;
				} else {
					insideTableId = source;
				}
				Fields fields = context.getComponentOutputFields(
						source.get_componentId(), source.get_streamId());
				Set<String> setFields = new HashSet<String>(fields.toList());
				if (joinKeyFields == null)
					joinKeyFields = setFields;
				else
					joinKeyFields.retainAll(setFields);

				for (String outfield : _outFields) {
					for (String sourcefield : fields) {
						if (outfield.equals(sourcefield)) {
							_fieldLocations.put(outfield, source);
						}
					}
				}
			}
			_joinKeyFields = new Fields(new ArrayList<String>(joinKeyFields));

			if (_fieldLocations.size() != _outFields.size()) {
				throw new RuntimeException(
						"Cannot find all outfields among sources");
			}
		}

		@Override
		public Map<String, Object> getComponentConfiguration() {
			Map<String, Object> conf = new HashMap<String, Object>();
			conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,
					emitFrequencyInSeconds);
			return conf;
		}

		private class OutsideExpireCallback implements
				RotatingMap.ExpiredCallback<List<Object>, Map<Tuple, Integer>> {
			@Override
			public void expire(List<Object> id, Map<Tuple, Integer> tuples) {
				// for ( List<Tuple> tupleList : tuples.values()) {

				/*
				 * for (Tuple tuple : tupleList) { //_collector.fail(tuple);
				 * 
				 * }
				 */
				// }

			}
		}

		private class InsideExpireCallback implements
				RotatingMap.ExpiredCallback<List<Object>, List<Tuple>> {
			@Override
			public void expire(List<Object> id, List<Tuple> tuples) {
				// for ( List<Tuple> tupleList : tuples.values()) {

				/*
				 * for (Tuple tuple : tupleList) { //_collector.fail(tuple);
				 * 
				 * }
				 */
				// }

			}
		}

	}
	
	public static class CountBolt extends BaseRichBolt {

		Map<String, Integer> results = null;
		OutputCollector _collector = null;
		
		@Override
		public void execute(Tuple tuple) {
			// TODO Auto-generated method stub
			String mktsegment = tuple.getString(0);
			Integer preResult = results.get(mktsegment);
			if( preResult == null) {
				preResult = 0;
			}
			preResult ++;
			results.put(mktsegment, preResult);
			_collector.ack(tuple);
			LOG.info("results: " + mktsegment + " :" + preResult);
		}

		@Override
		public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
			// TODO Auto-generated method stub
			_collector = arg2;
			results = new HashMap<String, Integer>();
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer arg0) {
			// TODO Auto-generated method stub
			
		}
		
	}
}
