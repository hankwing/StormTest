package storm.starter.tpch;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.Writer;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map.Entry;
import java.util.Set;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.log4j.Logger;

import com.esotericsoftware.minlog.Log;

import storm.starter.TPCHQuery3;
import storm.starter.bolt.SegVolBolt;
import storm.starter.tools.JoinWindowCounter;
import storm.starter.tools.NthLastModifiedTimeTracker;
import storm.starter.util.TupleHelpers;
import backtype.storm.Config;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.RotatingMap;

public class TPCH3 {
	
	public static long wholeSampleTime = 1800000;	// one parallisilm half an hour
	public static long intervalTime = 120000;	// 5 minites one sample
	public static long calThroughtInterval = 5000;  // calculate throughput every this millseconds
	public static long ignoreTime = 60000;
	public static String intermediateTopic = "tpchtemptopics";
	
	/*public static int supervisors = 4;		// supervisor number
	public static int cpucores = 96;
	public static int memory = 192;
	public static int onBoltNumber = 4;	
	public static int joinBoltNumber = 2;
	
	public static Number workers = 8;	// need random
	public static Number spouts = 8;	// need random
	public static Number onBolt = 16;	// need random
	public static Number joinBolt = 16;	// need random
	public static int windowLength = 30;	// need random
	public static int emitFrequency = 10;	// need random
*/
	public static class LineItemSplitBolt extends BaseBasicBolt {

		SimpleDateFormat df = null;
		Date fixDate = null;
		CollectSamples collectSamples = null;
		List<Integer> sampleVariable = null;
		
		public LineItemSplitBolt( List<Integer> sampleVariable) {
			this.sampleVariable = sampleVariable;
		}

		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			// TODO Auto-generated method stub
			super.prepare(stormConf, context);
			collectSamples = new CollectSamples( context , sampleVariable, true);
			collectSamples.startCollecting();
			
			df = new SimpleDateFormat("yyyy-MM-dd");
			try {
				fixDate = df.parse("1995-09-03");
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer arg0) {
			// TODO Auto-generated method stub
			arg0.declare(new Fields("orderkey", "extendedprice", "discount",
					"shipdate"));
		}

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			// TODO Auto-generated method stub
			
			collectSamples.incrementSpoutNum(); 	// add one to spout num
			String[] strings = input.getString(0).split("\\|");
			
			try {
				if (df.parse(strings[10]).before(fixDate)) {

					collector.emit(new Values(strings[0], Float
							.valueOf(strings[5]), Float.valueOf(strings[6]),
							strings[10]));

				}

				// _collector.ack(arg0);
			} catch (NumberFormatException | ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public static class OrdersSplitBolt extends BaseBasicBolt {

		SimpleDateFormat df = null;
		Date fixDate = null;
		CollectSamples collectSamples = null;
		List<Integer> sampleVariable = null;
		
		public OrdersSplitBolt( List<Integer> sampleVariable) {
			this.sampleVariable = sampleVariable;
		}

		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			// TODO Auto-generated method stub
			super.prepare(stormConf, context);
			collectSamples = new CollectSamples( context, sampleVariable, true );
			collectSamples.startCollecting();
			
			df = new SimpleDateFormat("yyyy-MM-dd");
			try {
				fixDate = df.parse("1995-09-03");
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer arg0) {
			// TODO Auto-generated method stub
			arg0.declare(new Fields("orderkey", "custkey", "orderdate",
					"shippriority"));
		}

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			// TODO Auto-generated method stub
			collectSamples.incrementSpoutNum(); 	// add one to spout num
			String[] strings = input.getString(0).split("\\|");
			try {
				if (df.parse(strings[4]).before(fixDate)) {

					collector.emit(new Values(strings[0], strings[1],
							strings[4], Integer.valueOf(strings[7])));
				}
				// _collector.ack(arg0);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public static class CustomerSplitBolt extends BaseBasicBolt {

		CollectSamples collectSamples = null;
		List<Integer> sampleVariable = null;
		
		public CustomerSplitBolt( List<Integer> sampleVariable) {
			this.sampleVariable = sampleVariable;
		}
		
		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			// TODO Auto-generated method stub
			super.prepare(stormConf, context);
			collectSamples = new CollectSamples( context,sampleVariable, true);
			collectSamples.startCollecting();
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer arg0) {
			// TODO Auto-generated method stub
			arg0.declare(new Fields("custkey", "mktsegment"));
		}

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			// TODO Auto-generated method stub
			collectSamples.incrementSpoutNum();
			String[] strings = input.getString(0).split("\\|");
			if (strings[6].equals("MACHINERY")) {

				collector.emit(new Values(strings[0], strings[6]));
			}
		}

	}

	/**
	 * join lineitem and orders with orderkey
	 * 
	 * @author hankwing
	 *
	 */
	public static class JoinBolt extends BaseRichBolt {

		Fields _joinKeyFields;
		Fields _outFields;
		int _numSources;
		GlobalStreamId outsideTableId = null;
		GlobalStreamId insideTableId = null;
		private JoinWindowCounter<Object> counter = null;
		private int windowLengthInSeconds;
		private int emitFrequencyInSeconds;
		//private boolean isNeedEmit = true;
		private OutputCollector _collector = null;

		Map<List<Object>, Map<Tuple, Integer>> outsideTable; // save
																// temp
																// data

		Map<List<Object>, List<Tuple>> insideTable;

		Map<String, GlobalStreamId> _fieldLocations;
		CollectSamples collectSamples = null;
		List<Integer> sampleVariable = null;

		public JoinBolt(Fields outFields, int windowLengthInSeconds,
				int emitFrequencyInSeconds, List<Integer> sampleVariable ) {
			this.windowLengthInSeconds = windowLengthInSeconds;
			this.emitFrequencyInSeconds = emitFrequencyInSeconds;
			this.sampleVariable = sampleVariable;
			_outFields = outFields;
			//isNeedEmit = needEmit;
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
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			// TODO Auto-generated method stub
			collectSamples = new CollectSamples( context, sampleVariable, false);
			collectSamples.startCollecting();
			counter = new JoinWindowCounter<Object>(deriveNumWindowChunksFrom(
					this.windowLengthInSeconds, this.emitFrequencyInSeconds));
			_collector = collector;
			_fieldLocations = new HashMap<String, GlobalStreamId>();
			outsideTable = new HashMap<List<Object>, Map<Tuple, Integer>>();
			insideTable = new HashMap<List<Object>, List<Tuple>>();
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

		@Override
		public void execute(Tuple input) {
			// TODO Auto-generated method stub
			if (TupleHelpers.isTickTuple(input)) {
				// LOG.info("Received tick tuple, triggering emit of current window counts");

				// remove expired tuple
				Map<Object, Map<GlobalStreamId, List<Tuple>>> tuplesToBeRemoved = counter
						.getRemoveTuplesThenAdvanceWindow();
				for (Object key : tuplesToBeRemoved.keySet()) {
					for (GlobalStreamId id : tuplesToBeRemoved.get(key)
							.keySet()) {
						if (id.equals(outsideTableId)) {
							// remove outside tuples

							for (Tuple removeTuple : tuplesToBeRemoved.get(key)
									.get(id)) {
								outsideTable.get((List<Object>) key).remove(
										removeTuple);
							}
							if (outsideTable.get((List<Object>) key).isEmpty()) {
								outsideTable.remove((List<Object>) key);
							}

						} else {

							insideTable.get((List<Object>) key).removeAll(
									tuplesToBeRemoved.get(key).get(id));
							if (insideTable.get((List<Object>) key).isEmpty()) {
								insideTable.remove((List<Object>) key);
							}

						}
					}
				}
				
				counter.resetAndadvanceHead(); // reset slot and remove empty
												// keys
				long outsideTableValue = 0;
				long insideTableValue = 0;
				tuplesToBeRemoved.clear();
				/*
				 * for( Map<Tuple, Integer> value :outsideTable.values()) {
				 * outsideTableValue += value.size(); } for( Map<Tuple, Integer>
				 * value :outsideTable.values()) { insideTableValue +=
				 * value.size(); } if( isNeedEmit) { Log.info("outside value:" +
				 * outsideTableValue); Log.info("inside value:" +
				 * insideTableValue); }
				 */

			} else {

				collectSamples.incrementSpoutNum();
				List<Object> joinKey = input.select(_joinKeyFields);

				Map<Tuple, Integer> outsideParts = outsideTable.get(joinKey);
				List<Tuple> insideParts = insideTable.get(joinKey);
				GlobalStreamId streamId = new GlobalStreamId(
						input.getSourceComponent(), input.getSourceStreamId());

				counter.addToSlot(joinKey, streamId, input); // add to slot

				if (outsideTableId.equals(streamId)) {
					// receive outside table tuple
					if (outsideParts == null) {
						outsideParts = new HashMap<Tuple, Integer>();
						outsideParts.put(input, 0);
						outsideTable.put(joinKey, outsideParts);

					} else {
						outsideParts.put(input, 0);
					}
				} else {
					// receive inside table tuple
					if (insideParts == null) {
						insideParts = new ArrayList<Tuple>();
						insideParts.add(input);
						insideTable.put(joinKey, insideParts);

					} else {
						insideParts.add(input);
					}

				}

				if (outsideParts != null && insideParts != null) {
					for (Entry<Tuple, Integer> tupleWithInteger : outsideParts
							.entrySet()) {

						int nowIndex = tupleWithInteger.getValue();

						while (nowIndex < insideParts.size()) {
							// join
							List<Object> joinResult = new ArrayList<Object>();
							// List<Tuple> anchorTuples = new
							// ArrayList<Tuple>();
							for (String outField : _outFields) {
								GlobalStreamId loc = _fieldLocations
										.get(outField);
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

							// LOG.info("join result: " +
							// joinResult.toString());
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
			_collector.ack(input);
		}

	}

	
	/**
	 * use join result to calculate sum
	 * @author hankwing
	 *
	 */
	public static class GroupByKeyEmitResults extends BaseBasicBolt {

		private SlidingWindowCounter<Object> counter;
		private int windowLengthInSeconds;
		private int emitFrequencyInSeconds;
		CollectSamples collectSamples = null;
		List<Integer> sampleVariable = null;
		
		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			// TODO Auto-generated method stub
			super.prepare(stormConf, context);
			collectSamples = new CollectSamples( context, sampleVariable, true);
			collectSamples.startCollecting();
		}

		public GroupByKeyEmitResults(int windowLengthInSeconds,
				int emitFrequencyInSeconds, List<Integer> sampleVariable) {
			this.windowLengthInSeconds = windowLengthInSeconds;
			this.emitFrequencyInSeconds = emitFrequencyInSeconds;
			this.sampleVariable = sampleVariable;
			counter = new SlidingWindowCounter<Object>(
					deriveNumWindowChunksFrom(this.windowLengthInSeconds,
							this.emitFrequencyInSeconds));
		}

		private int deriveNumWindowChunksFrom(int windowLengthInSeconds,
				int windowUpdateFrequencyInSeconds) {
			return windowLengthInSeconds / windowUpdateFrequencyInSeconds;
		}

		private void emitCurrentWindowCounts() {
			Map<Object, Float> counts = counter.getResultsThenAdvanceWindow();
			for (Entry<Object, Float> entry : counts.entrySet()) {
				List<String> obj = (List<String>) entry.getKey();
				Float sumOfRevenue = entry.getValue();
				/*Log.info("Results : " + obj.get(0) + " " + obj.get(1)
				 + " " + obj.get(2) + " " + sumOfRevenue);*/
				// collector.emit(new Values(obj.get(0),obj.get(1),obj.get(2),
				// count, actualWindowLengthInSeconds));
			}
		}

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			// TODO Auto-generated method stub
			if (TupleHelpers.isTickTuple(input)) {
				// LOG.info("Received tick tuple, triggering emit of current window counts");
				emitCurrentWindowCounts();
			} else {
				collectSamples.incrementSpoutNum();
				List<String> obj = new ArrayList<String>();
				obj.add((String) input.getValue(0));
				obj.add((String) input.getValue(1));
				obj.add(String.valueOf( input.getValue(2)));
				obj.add(String.valueOf( input.getValue(3)));
				obj.add(String.valueOf( input.getValue(4)));
				counter.addAndCalculateDataToCount(obj);
			}
		}
		
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("orderkey", "revenue", "orderdate", "shippriority"));
		}

		@Override
		public Map<String, Object> getComponentConfiguration() {
			Map<String, Object> conf = new HashMap<String, Object>();
			conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,
					emitFrequencyInSeconds);
			return conf;
		}
	}


	public static class SlidingWindowCounter<T> implements Serializable {

		private SlotBasedCounter<T> objCounter;
		private int headSlot;
		private int tailSlot;
		private int windowLengthInSlots;

		public SlidingWindowCounter(int windowLengthInSlots) {
			if (windowLengthInSlots < 2) {
				throw new IllegalArgumentException(
						"Window length in slots must be at least two (you requested "
								+ windowLengthInSlots + ")");
			}
			this.windowLengthInSlots = windowLengthInSlots;
			this.objCounter = new SlotBasedCounter<T>(this.windowLengthInSlots);

			this.headSlot = 0;
			this.tailSlot = slotAfter(headSlot);
		}

		public void addAndCalculateDataToCount(T obj) {
			float result = Float.valueOf(((List<String>) obj).get(3)) * 
					(1 - Float.valueOf(((List<String>) obj).get(4)));
			
			objCounter.addAndCalculateData(obj, headSlot, result);
		}

		public Map<T, Float> getResultsThenAdvanceWindow() {
			Map<T, Float> counts = objCounter.getResults();
			objCounter.wipeZeros();
			objCounter.wipeSlot(tailSlot);
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

	public static class SlotBasedCounter<T> implements Serializable {

		//private static final long serialVersionUID = 4858185737378394432L;

		private final Map<T, float[]> objToCounts = new HashMap<T, float[]>();
		private final int numSlots;

		public SlotBasedCounter(int numSlots) {
			if (numSlots <= 0) {
				throw new IllegalArgumentException(
						"Number of slots must be greater than zero (you requested "
								+ numSlots + ")");
			}
			this.numSlots = numSlots;
		}

		public void addAndCalculateData(T obj, int slot, float number) {
			float[] counts = objToCounts.get(obj);
			if (counts == null) {
				counts = new float[this.numSlots];
				objToCounts.put(obj, counts);
			}
			counts[slot] += number;
		}

		public Map<T, Float> getResults() {
			Map<T, Float> result = new HashMap<T, Float>();
			for (T obj : objToCounts.keySet()) {
				result.put(obj, computeTotalCount(obj));
			}
			return result;
		}

		private float computeTotalCount(T obj) {
			float[] curr = objToCounts.get(obj);
			float total = 0;
			for (float l : curr) {
				total += l;
			}
			return total;
		}

		/**
		 * Reset the slot count of any tracked objects to zero for the given
		 * slot.
		 *
		 * @param slot
		 */
		public void wipeSlot(int slot) {
			for (T obj : objToCounts.keySet()) {
				resetSlotCountToZero(obj, slot);
			}
		}

		private void resetSlotCountToZero(T obj, int slot) {
			float[] counts = objToCounts.get(obj);
			counts[slot] = 0;
		}

		private boolean shouldBeRemovedFromCounter(T obj) {
			return computeTotalCount(obj) == 0;
		}

		/**
		 * Remove any object from the counter whose total count is zero (to free
		 * up memory).
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
	
	/**
	 * collect samples
	 * @author hankwing
	 *
	 */
	public static class CollectSamples {
		
		long spoutNum = 0;
		long controlSpeedNum = 0;
		int avgThroughout = 0;
		int throughtNum = 0;
		TopologyContext context = null;
		boolean isOnBolt = false;
		Random random = null;
		//Writer writer = null;
		//Writer latencyWriter = null;
		//FileOutputStream fos = null;
		//FileOutputStream latencyFos = null;
		Timer calThroughput = null;
		Timer printSpoutSpeed = null; // change spout speed every 5min
		Timer writeLatency = null;
		Calendar before = null;
		// Calendar after = null;
		DateFormat df = null;
		long beginSleep = 0;
		boolean isIgnore = true;
		
		public static int supervisors = 4;		// supervisor number
		public static int cpucores = 96;
		public static int memory = 192;
		public static int onBoltNumber = 4;	
		public static int joinBoltNumber = 2;
		public static int kafkaBrokers = 5;		// new add
		public static int kafkaPartitions = 32;
		
		public static int workers = 8;	// need random
		public static int spouts = 8;	// need random
		public static int onBolt = 16;	// need random
		public static int joinBolt = 16;	// need random
		public static int windowLength = 30;	// need random
		public static int emitFrequency = 10;	// need random
		
		
		Tools tools  = null;
		double avgCpu = 0.0;
		long avgMemory = 0;
		
		public CollectSamples( TopologyContext context, List<Integer> sampleVariable, boolean isOnBolt  ) {
			this.isOnBolt = isOnBolt;
			this.context = context;
			supervisors = sampleVariable.get(0);
			cpucores = sampleVariable.get(1);
			memory = sampleVariable.get(2);
			onBoltNumber = sampleVariable.get(3);
			joinBoltNumber = sampleVariable.get(4);
			kafkaBrokers = sampleVariable.get(5);
			kafkaPartitions = sampleVariable.get(6);
			workers = sampleVariable.get(7);
			spouts = sampleVariable.get(8);
			onBolt = sampleVariable.get(9);
			joinBolt = sampleVariable.get(10);
			windowLength = sampleVariable.get(11);
			emitFrequency = sampleVariable.get(12);
		}
		
		// add one to spoutNum
		public void incrementSpoutNum() {
			spoutNum ++;
		}
		
		public void startCollecting() {
			
			tools = new Tools();
			df = new SimpleDateFormat("dd HH:mm");
			before = Calendar.getInstance();
			before.set(Calendar.MINUTE, before.get(Calendar.MINUTE) + 1);
			before.set(Calendar.SECOND, 0);
			beginSleep = before.getTimeInMillis()
					- Calendar.getInstance().getTimeInMillis();
			calThroughput = new Timer();
			printSpoutSpeed = new Timer();
			writeLatency = new Timer();
			try {
				final int taskCount = isOnBolt?
						context.getComponentTasks(context.getThisComponentId()).size()
						* TPCHQuery3.onBoltNumber:
							context.getComponentTasks(context.getThisComponentId()).size() 
							* TPCHQuery3.joinBoltNumber;
				// Build the configuration required for connecting to Kafka
				Properties props = new Properties();
				props.put("metadata.broker.list", "192.168.0.19:9092,192.168.0.21:9092,"
						+ "192.168.0.22:9092,192.168.0.23:9092,192.168.0.24:9092");
				//props.put("topic.metadata.refresh.interval.ms", "2000");
				props.put("serializer.class", "kafka.serializer.StringEncoder");
				props.put("request.required.acks", "1");	// guarantee the message be sent
				// Create the producer instance
				ProducerConfig config = new ProducerConfig(props);
				final Producer<String, String> producer = new Producer<String, String>(config);
				
				/*fos = isOnBolt ? new FileOutputStream(
						"/home/wamdm/wengzujian/stormResult/onBolt_"
								+ context.getThisTaskId()) : new FileOutputStream(
										"/home/wamdm/wengzujian/stormResult/joinBolt_"
												+ context.getThisTaskId());
				writer = new BufferedWriter(
						new OutputStreamWriter(fos, "utf-8"));
				writer.write("supervisors, cpucores, memory(G), workers, onBoltNumber, "
						+ "joinBoltNumber, onBoltParallism, joinBoltParallism, spoutsParallism,"
						+ "windowLength, emitFrequency, onBoltRate, joinBoltRate\n");
				writer.write(supervisors + "," + 
						cpucores + "," + memory + ","
						+ workers + "," + onBoltNumber + "," + 
						joinBoltNumber + "," + onBolt
						+ "," + joinBolt + "," + spouts + ","
						+ windowLength + "," + emitFrequency + "\n");*/
				
				printSpoutSpeed.schedule(new TimerTask() {

					@Override
					public void run() {
						// TODO Auto-generated method stub
						if (before.get(Calendar.SECOND) > 30) {
							before.set(Calendar.MINUTE,
									before.get(Calendar.MINUTE) + 1);
						}

						try {
							//if( _context.getThisTaskIndex() == 1) {
								/*HashMap hm = (HashMap) context
									.getRegisteredMetricByName(
											"__complete-latency")
									.getValueAndReset();
								
								int completeLatency = ((Double)hm.get("default")).intValue();*/
								String dataString = df.format(before.getTime()) + ","
										+ avgThroughout + "\n";
								//writer.write(dataString);
								//writer.flush();
								
								KeyedMessage<String, String> data = isOnBolt ? 
										new KeyedMessage<String, String>(		
										TPCH3.intermediateTopic, "onBolt,"+ taskCount+"," + dataString
										+ "," + avgCpu + "," + avgMemory + "," + supervisors + "," 
										+ cpucores + "," + memory + "," + workers + "," + onBoltNumber 
										+ "," + joinBoltNumber + "," + kafkaBrokers + ","
										+ kafkaPartitions + ","
 										+ onBolt +"," + joinBolt + "," 
										+ spouts +"," + windowLength +"," + emitFrequency ) : 
										new KeyedMessage<String, String>(
										TPCH3.intermediateTopic, "joinBolt,"+ taskCount+"," + dataString
										+ "," + avgCpu + "," + avgMemory  + "," + supervisors + "," 
										+ cpucores + "," + memory + "," + workers + "," + onBoltNumber 
										+ "," + joinBoltNumber + + kafkaBrokers + ","
										+ kafkaPartitions + 
										"," + onBolt +"," + joinBolt + "," 
										+ spouts +"," + windowLength +"," + emitFrequency );
								// send intermediate data to kafka topic
								//if(!isIgnore ) {
									producer.send(data);
								//}
								//else {
								//	isIgnore = false;
								//}
								
							//}
							//else {
							//	writer.write(df.format(before.getTime()) + ","
							//			+ avgThroughout
							//			+ "\n");
							//	writer.flush();
							//}
							
							/*
							 * if( avgThroughout == 0) { reSet();
							 * 
							 * }
							 */
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						avgThroughout = throughtNum = 0;
						avgCpu = avgMemory = 0;
						before = Calendar.getInstance();
					}

				},  intervalTime, intervalTime);
				
				// latency writer
				/*writeLatency.schedule(new TimerTask() {

					@Override
					public void run() {
						// TODO Auto-generated method stub
						try {
							latencyFos = new FileOutputStream(
									"/home/wamdm/wengzujian/stormResult/latency");
							latencyWriter = new BufferedWriter(
									new OutputStreamWriter(latencyFos, "utf-8"));
							latencyWriter.write(TPCHQuery3.supervisors + "," + 
									"".cpucores + "," + "".memory + ","
									+ "".workers + "," + "".onBoltNumber + "," + 
									"".joinBoltNumber + "," + "".onBolt
									+ "," + "".joinBolt + "," + "".spouts + ","
									+ "".windowLength + "," + "".emitFrequency + ",");
							HashMap hm = (HashMap) context
							.getRegisteredMetricByName(
									"__complete-latency")
							.getValueAndReset();
					
							int completeLatency = ((Double)hm.get("default")).intValue();
							latencyWriter.write(completeLatency + "\n");
							latencyWriter.flush();
							latencyWriter.close();
							
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
					}
					
				}, wholeSampleTime);*/

				calThroughput.schedule(new TimerTask() {

					@Override
					public void run() {
						// TODO Auto-generated method stub
						// LOG .info("latency : " + hm.get( "default"));
						/*if( throughtNum == 0) {
							// ignore the first begin time metric
							throughtNum = 1;
							return;
						}*/
							
						if (avgThroughout == 0) {
							
							avgCpu = tools.getCpuUsage();		// get cpu metric
							avgMemory = tools.getMemoryUsage();
							avgThroughout = (int) (spoutNum / (calThroughtInterval/ 1000));
							throughtNum = 1;
						} else {
							
							avgCpu = ( avgCpu * throughtNum + tools.getCpuUsage() ) / (throughtNum +1);
							avgMemory = ( avgMemory * throughtNum + tools.getMemoryUsage() ) / (throughtNum +1);
							avgThroughout = (int) ((avgThroughout * throughtNum + (spoutNum 
									/ (calThroughtInterval / 1000))) / (throughtNum + 1));
							throughtNum++;
						}
						// LOG.info("avg throughput: " + sleepInterval + ": "
						// +evgThroughout);
						spoutNum = 0;
					}

				}, calThroughtInterval , calThroughtInterval);

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
