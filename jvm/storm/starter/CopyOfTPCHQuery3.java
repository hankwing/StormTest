package storm.starter;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;

import org.apache.log4j.Logger;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.starter.tpch.TPCH1;
import storm.starter.tpch.TPCH3;
import storm.starter.tpch.TPCHKafkaSpouts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
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
import backtype.storm.utils.Utils;

/**
 * 
 * @author hankwing
 * 
 */
public class CopyOfTPCHQuery3 {

	private static Logger LOG = Logger.getLogger(tcphTest.class);
	private static int supervisors = 3;
	private static int cpucores = 4;
	private static String memory = "4";
	private static Number workers = 8;
	private static Number spouts = 8;
	private static Number bolts = 16;
	private static long intervalTime = 300000;

	public static void main(String[] args) {
		
		// zookeeper hosts for the Kafka cluster
		if (args != null && args.length > 2) {
			workers = Integer.valueOf(args[1]);
			spouts = Integer.valueOf(args[2]);
			bolts = Integer.valueOf(args[3]);

		}

		ZkHosts zkHosts = new ZkHosts(
				"192.168.0.73:2181,192.168.0.21:2181,192.168.0.22:2181");
				//"192.168.0.72:2181");
		
		// lineItem kafka spout
		SpoutConfig lineItemKafkaConfig = new SpoutConfig(zkHosts, "lineitems", "/lineitems",
				UUID.randomUUID().toString());
		lineItemKafkaConfig.bufferSizeBytes = 10 * 1024 * 1024;
		lineItemKafkaConfig.fetchSizeBytes = 10 * 1024 * 1024;
		// Specify that the kafka messages are String
		lineItemKafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		lineItemKafkaConfig.forceFromStart = true;
		
		// orders kafka spout
		SpoutConfig ordersKafkaConfig = new SpoutConfig(zkHosts, "orders", "/orders",
				UUID.randomUUID().toString());
		ordersKafkaConfig.bufferSizeBytes = 10 * 1024 * 1024;
		ordersKafkaConfig.fetchSizeBytes = 10 * 1024 * 1024;
		// Specify that the kafka messages are String
		ordersKafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		ordersKafkaConfig.forceFromStart = true;
		
		// customers kafka spout
		SpoutConfig customersKafkaConfig = new SpoutConfig(zkHosts, "customers", "/customers",
				UUID.randomUUID().toString());
		customersKafkaConfig.bufferSizeBytes = 10 * 1024 * 1024;
		customersKafkaConfig.fetchSizeBytes = 10 * 1024 * 1024;
		// Specify that the kafka messages are String
		customersKafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		customersKafkaConfig.forceFromStart = true;
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("tickSpout", new TickSpout(spouts.intValue()), 1);
		builder.setBolt("changeRateBolt", new ChangeRateBolt(), workers)
				.allGrouping("tickSpout");
		// spouts
		/*builder.setSpout(
				"lineItemkafkaSpout",
				new TPCHKafkaSpouts.GeneralKafkaSpout(lineItemKafkaConfig, workers.intValue(), spouts
						.intValue(), bolts.intValue(), supervisors, cpucores, memory, intervalTime), spouts);
		builder.setSpout(
				"orderskafkaSpout",
				new TPCHKafkaSpouts.GeneralKafkaSpout(ordersKafkaConfig, workers.intValue(), spouts
						.intValue(), bolts.intValue(), supervisors, cpucores, memory, intervalTime), spouts);
		builder.setSpout(
				"customerskafkaSpout",
				new TPCHKafkaSpouts.GeneralKafkaSpout(customersKafkaConfig, workers.intValue(), spouts
						.intValue(), bolts.intValue(), supervisors, cpucores, memory, intervalTime), spouts);*/
		
		// tpch3 bolts
		/*builder.setBolt("lineItemSplitBolt", new TPCH3.LineItemSplitBolt(), bolts)
		.shuffleGrouping("lineItemkafkaSpout");
		builder.setBolt("ordersSplitBolt", new TPCH3.OrdersSplitBolt(), bolts)
		.shuffleGrouping("orderskafkaSpout");
		builder.setBolt("customerSplitBolt", new TPCH3.CustomerSplitBolt(), bolts)
		.shuffleGrouping("customerskafkaSpout");
		*/
	/*	builder.setBolt("lineitemAndOrdersJoinBolt", 
				new TPCH3.JoinBolt(new Fields("orderkey", "extendedprice","discount","shipdate",
						"custkey","orderdate","shippriority")
				, 500, 50), bolts).fieldsGrouping("lineItemSplitBolt", new Fields("orderkey"))
				.fieldsGrouping("ordersSplitBolt", new Fields("orderkey"));
		
		builder.setBolt("customerAndOrdersJoinBolt", 
				new TPCH3.JoinBolt(new Fields("orderkey", "orderdate","shippriority","extendedproce",
						"discount")
				, 500, 50), bolts).fieldsGrouping("lineitemAndOrdersJoinBolt", new Fields("custkey"))
				.fieldsGrouping("customerSplitBolt", new Fields("custkey"));*/
		
		Config conf = new Config();
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 10*1024);
		conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, false);
		// conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, spouts.intValue());
		// conf.setDebug(true);

		if (args != null && args.length > 0) {

			conf.setNumWorkers(workers.intValue());
			try {
				StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
						builder.createTopology());
			} catch (AlreadyAliveException | InvalidTopologyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			conf.setNumWorkers(2);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(10000000);
			cluster.killTopology("test");
			cluster.shutdown();
		}

	}

	/**
	 * control the spout rate in one machine
	 * 
	 * @author hankwing
	 *
	 */
	public static class TickSpout extends BaseRichSpout {

		Timer tick = null;
		Random random = null;
		SpoutOutputCollector _collector = null;
		boolean isStart = true;
		int spouts;

		public TickSpout(int spouts) {
			this.spouts = spouts;
		}

		@Override
		public void nextTuple() {
			// TODO Auto-generated method stub
			if (isStart) {
				isStart = false;
				Calendar cd = Calendar.getInstance();
				cd.set(Calendar.MINUTE, cd.get(Calendar.MINUTE) + 1);
				cd.set(Calendar.SECOND, 0);
				Utils.sleep(cd.getTimeInMillis()
						- Calendar.getInstance().getTimeInMillis()
						+ intervalTime);
			} else {
				Utils.sleep(intervalTime);
			}
			// 250000 1200
			_collector.emit(new Values(random.nextInt(300000 / spouts), random
					.nextInt(1500)));
		}

		@Override
		public void open(Map arg0, TopologyContext arg1,
				SpoutOutputCollector arg2) {
			// TODO Auto-generated method stub
			random = new Random();
			_collector = arg2;
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer arg0) {
			// TODO Auto-generated method stub
			arg0.declare(new Fields("spoutInterval", "sleepTime"));
		}

	}

	public static class ChangeRateBolt extends BaseRichBolt {

		Random random = null;
		TopologyContext _context = null;

		@Override
		public void execute(Tuple arg0) {
			// TODO Auto-generated method stub
			TPCHKafkaSpouts.spoutInterval = arg0.getInteger(0);
			TPCHKafkaSpouts.sleepTime = arg0.getInteger(1);
					
		}

		@Override
		public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
			// TODO Auto-generated method stub
			_context = arg1;
			random = new Random();
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer arg0) {
			// TODO Auto-generated method stub

		}

	}

}
