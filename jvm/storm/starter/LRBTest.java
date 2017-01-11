package storm.starter;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Semaphore;

import storm.starter.bolt.CarTollJoinBolt;
import storm.starter.bolt.CurActiveCarsBolt;
import storm.starter.bolt.SegAvgSpeedBolt;
import storm.starter.bolt.SegVolBolt;
import storm.starter.bolt.WayJoinBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.sun.istack.internal.logging.Logger;

/**
 * test the lrb data
 * 
 * @author hankwing
 * 
 */
public class LRBTest {

	private static Logger LOG = Logger.getLogger(LRBTest.class);

	public static void main(String[] args) {

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("cardatapointsspout", new CarDataPointsSpout(),48);
		builder.setBolt("segvolbolt", new SegVolBolt(30, 10),8).fieldsGrouping(
				"cardatapointsspout", new Fields("xway", "dir", "seg"));
		builder.setBolt("segavgspeedbolt", new SegAvgSpeedBolt(30, 10),8)
				.fieldsGrouping("cardatapointsspout",
						new Fields("xway", "dir", "seg"));
		builder.setBolt("curactivecarsbolt", new CurActiveCarsBolt(60, 30), 8)
				.fieldsGrouping("cardatapointsspout",
						new Fields("carid", "xway", "dir", "seg"));
		builder.setBolt("wayjoinbolt", new WayJoinBolt(),8).setNumTasks(16)
				.fieldsGrouping("segvolbolt", new Fields("xway", "dir", "seg"))
				.fieldsGrouping("segavgspeedbolt",
						new Fields("xway", "dir", "seg"));
		builder.setBolt("cartolljoinbolt", new CarTollJoinBolt(), 8).setNumTasks(16)
				.fieldsGrouping("wayjoinbolt",
						new Fields( "xway", "dir", "seg"))
				.fieldsGrouping("curactivecarsbolt",
						new Fields("xway", "dir", "seg"));

		// builder.setBolt("countbolt", new CountBolt())
		// .fieldsGrouping("joinbolt", new Fields("mktsegment"));

		Config conf = new Config();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
		// conf.setDebug(true);

		if (args != null && args.length > 0) {

			conf.setNumWorkers(8);
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
	 * read the cardatapoints data file and send it to storm
	 * 
	 * @author hankwing
	 * 
	 */
	public static class CarDataPointsSpout extends BaseRichSpout {

		SpoutOutputCollector _collector = null;
		BufferedReader br = null;
		Integer startTime = 0;
		Semaphore semp = new Semaphore(0); // communicate two threads

		@Override
		public void nextTuple() {
			// TODO Auto-generated method stub
			// Utils.sleep(100);

			try {
				new SpoutThreadWithin3Seconds(br, _collector, startTime, semp)
						.start();
				semp.acquire();
				startTime += 10;
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		@Override
		public void open(Map arg0, TopologyContext arg1,
				SpoutOutputCollector arg2) {
			// TODO Auto-generated method stub
			try {
				FileReader fr = new FileReader(
						"/home/wamdm/wengzujian/lrbNfsData/cardatapoints.out");
				
				/*FileReader fr = new FileReader(
						"tpch/LRBData/cardatapoints.out");*/
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
			arg0.declare(new Fields("type", "time", "carid", "speed", "xway",
					"lane", "dir", "seg", "pos"));
		}

	}

	/**
	 * send tuples whose timestamps are within 3 seconds
	 * 
	 * @author hankwing
	 * 
	 */
	public static class SpoutThreadWithin3Seconds extends Thread {

		private SpoutThreadWithin3Seconds mInstance = null;
		private BufferedReader br = null;
		private SpoutOutputCollector collector = null;
		private Integer startTime;
		private Semaphore semp;
		private long spoutSpeed = 0;

		public SpoutThreadWithin3Seconds(BufferedReader br,
				SpoutOutputCollector collector, Integer startTime,Semaphore semp) {
			this.br = br;
			this.collector = collector;
			this.startTime = startTime;
			this.semp = semp;
			mInstance = this;

		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			Timer timer = new Timer();
			timer.schedule(new TimerTask() {
				@Override
				public void run() {

					//System.out.println("thread interrupted!");
					//LOG.info("spout speed: " + spoutSpeed);
					mInstance.interrupt();
					semp.release();
				}
			}, 10 * 1000);

			String data = null;
			try {
				for (data = br.readLine(); Integer.valueOf(data.split(",")[1]) < startTime + 10; data = br
						.readLine()) {
					String datas[] = data.split(",");
					if (Integer.valueOf(datas[0].compareTo("0")) == 0) {
						collector.emit(new Values(datas[0], datas[1], datas[2],
								datas[3], datas[4], datas[5], datas[6],
								datas[7], datas[8])); // send datas
						spoutSpeed ++;
					}

					br.mark(100);
				}
				br.reset();
			} catch (NumberFormatException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			super.run();
		}

	}

}
