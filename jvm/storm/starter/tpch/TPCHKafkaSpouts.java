package storm.starter.tpch;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.starter.TPCHQuery3;
import storm.starter.tools.GetStormUiMetrics;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;

/**
 * kafka spout for tpch relations
 * @author hankwing
 *
 */
public class TPCHKafkaSpouts {

	/**
	 * control throughput
	 * 
	 * @author hankwing
	 *
	 */
	
	public static int spoutInterval = Integer.MAX_VALUE; // from 1 to 500
	public static int sleepTime = 0;
	
	public static class GeneralKafkaSpout extends KafkaSpout {

		Map _conf = null;
		TopologyContext _context = null;
		SpoutOutputCollector _collector = null;
		long controlSpeedNum = 0;
		long spoutNum = 0;
		int avgThroughout = 0;
		
		double avgCPU = 0;
		long avgMemory = 0;
		
		int fixedAvgThroughout = 0;		// max throughput (per second)
		int throughtNum = 0;
		Timer calThroughput = null;
		Timer stopCollectAndChangeRate = null;
		long beginSleep = 0;
		Calendar before = null;
		// Calendar after = null;
		DateFormat df = null;
		int sampleNumber = 1;
		int sampleTotal = 0;
		Writer writer = null;
		FileOutputStream fos = null;
		List<Integer> sampleVariable = null;
		Random randomno = null;
		Tools tools  = null;
		GetStormUiMetrics stormUiMetrics = null;
		boolean isIgnore = true;
		
		public GeneralKafkaSpout(SpoutConfig spoutConf) {
			super(spoutConf);
			// TODO Auto-generated constructor stub
			
		}
		
		@Override
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			// TODO Auto-generated method stub
			stormUiMetrics = new GetStormUiMetrics();
			tools = new Tools();
			_conf = conf;
			_context = context;
			_collector = collector;
			sampleTotal = (int) (TPCH3.wholeSampleTime / TPCH3.intervalTime);
			
			randomno = new Random();
			df = new SimpleDateFormat("dd HH:mm");
			before = Calendar.getInstance();
			before.set(Calendar.MINUTE, before.get(Calendar.MINUTE) + 1);
			before.set(Calendar.SECOND, 0);
			beginSleep = before.getTimeInMillis()
					- Calendar.getInstance().getTimeInMillis();
			calThroughput = new Timer();
			stopCollectAndChangeRate = new Timer();
			
			calThroughput.schedule(new TimerTask() {

				@Override
				public void run() {
					// TODO Auto-generated method stub
					// LOG .info("latency : " + hm.get( "default"));

					if (avgThroughout == 0) {
						
						avgCPU = tools.getCpuUsage();		// get cpu metric
						avgMemory = tools.getMemoryUsage();
						avgThroughout = (int) (spoutNum / (TPCH3.calThroughtInterval/ 1000) );
						throughtNum = 1;
					} else {
						avgCPU = ( avgCPU * throughtNum + tools.getCpuUsage() ) / (throughtNum +1);
						avgMemory = ( avgMemory * throughtNum + tools.getMemoryUsage() ) / (throughtNum +1);
						avgThroughout = (int) ((avgThroughout * throughtNum +(spoutNum 
								/ (TPCH3.calThroughtInterval / 1000))) / (throughtNum + 1));
						throughtNum++;
					}
					// LOG.info("avg throughput: " + sleepInterval + ": "
					// +evgThroughout);
					spoutNum = 0;
				}

			}, TPCH3.calThroughtInterval , TPCH3.calThroughtInterval);
			
			try {
				
				// task count
				final int taskCount = context.getComponentTasks(context.getThisComponentId()).size()
						* 3;	// 3 is spout number
				// Build the configuration required for connecting to Kafka
				Properties props = new Properties();
				
				// List of Kafka brokers. Complete list of brokers is not
				// required as the producer will auto discover the rest of
				// the brokers. Change this to suit your deployment.
				props.put("metadata.broker.list", "192.168.0.19:9092,192.168.0.21:9092,"
						+ "192.168.0.22:9092,192.168.0.23:9092,192.168.0.24:9092");
				//props.put("partitioner.class", "storm.starter.kafka.SimplePartitioner");
				// Serializer used for sending data to kafka. Since we are sending
				// string,
				// we are using StringEncoder.
				//props.put("topic.metadata.refresh.interval.ms", "2000");
				props.put("serializer.class", "kafka.serializer.StringEncoder");
				props.put("request.required.acks", "1");	// guarantee the message be sent
				// Create the producer instance
				ProducerConfig config = new ProducerConfig(props);
				final Producer<String, String> producer = new Producer<String, String>(config);

				//fos = new FileOutputStream(
				//		"/home/wamdm/wengzujian/stormResult/ThroughputAndLatency_"
				//				+ context.getThisTaskId());
				//writer = new BufferedWriter(
				//		new OutputStreamWriter(fos, "utf-8"));
				
				// stop collect max throughput after 5 minutes
				stopCollectAndChangeRate.schedule(new TimerTask() {

					@Override
					public void run() {
						// TODO Auto-generated method stub
						if( sampleNumber == 1) {
							fixedAvgThroughout = avgThroughout;
						}
						
						if (before.get(Calendar.SECOND) > 30) {
							before.set(Calendar.MINUTE,
									before.get(Calendar.MINUTE) + 1);
						}

						try {
							
							/*HashMap hm = (HashMap) _context
									.getRegisteredMetricByName(
											"__complete-latency")
									.getValueAndReset();*/
							
							//Double completeLatency = stormUiMetrics.getSpoutLatency();
							int completeLatency = 0;
							String dataString = df.format(before.getTime()) + ","
									+ avgThroughout + "," +completeLatency +"\n";
							//writer.write(dataString);
							//writer.flush();
							
							KeyedMessage<String, String> data = new KeyedMessage<String, String>(
									TPCH3.intermediateTopic, "spoutRate," + taskCount + ","+ dataString
									+ "," + avgCPU + "," + avgMemory);
							// send intermediate data to kafka topic
							//if(!isIgnore) {
								producer.send(data);
							//}
							//else {
							//	isIgnore = false;
							//}

						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						avgThroughout = throughtNum = 0;
						avgCPU = avgMemory = 0;
						before = Calendar.getInstance();
						
						//double randomDouble = randomno.nextDouble();
						//spoutInterval = fixedAvgThroughout / sampleTotal * sampleNumber;
						//sleepTime = 1000 * ( 1 - sampleNumber / sampleTotal);
						//spoutInterval = (int) (fixedAvgThroughout * randomno.nextDouble());
						//sleepTime = (int) (1000 * randomno.nextDouble());
						sampleNumber ++;		
					}
					
				}, TPCH3.intervalTime, TPCH3.intervalTime);
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			super.open(conf, context, collector);
		}

		@Override
		public void nextTuple() {
			// TODO Auto-generated method stub
			super.nextTuple();
			// LOG.info("spoutNum: " + super.spoutNumOneTime.getNum());
			spoutNum = spoutNum + spoutNumOneTime.getNum();
			// LOG.info("spout Number!:" + spoutNum);
			spoutNumOneTime.reset();
			/*if (controlSpeedNum++ > spoutInterval) {
				Utils.sleep(sleepTime);
				controlSpeedNum = 0;
			}*/
		}
		
	}
}
