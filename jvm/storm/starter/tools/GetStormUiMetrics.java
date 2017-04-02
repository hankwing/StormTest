package storm.starter.tools;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.thrift7.TException;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.transport.TFramedTransport;
import org.apache.thrift7.transport.TSocket;
import org.apache.thrift7.transport.TTransportException;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.ExecutorSpecificStats;
import backtype.storm.generated.ExecutorStats;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.SpoutStats;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;

/**
 * get storm ui metrics
 * 
 * @author hankwing
 *
 */
public class GetStormUiMetrics {

	private ClusterSummary summary = null;
	private Client client = null;

	public static void main(String[] args) {
		GetStormUiMetrics stormUiMetrics = new GetStormUiMetrics();
		System.out.println( "spout Latency: " + stormUiMetrics.getSpoutLatency());
	}
	
	public GetStormUiMetrics() {
		TSocket socket = new TSocket("192.168.0.100", 6627);
		TFramedTransport transport = new TFramedTransport(socket);
		TBinaryProtocol protocol = new TBinaryProtocol(transport);
		client = new Client(protocol);
		try {
			transport.open();
			summary = client.getClusterInfo();

			// Cluster Details
			System.out.println("**** Storm UI Home Page ****");
			System.out.println(" ****Cluster Summary**** ");
			int nimbusUpTime = summary.get_nimbus_uptime_secs();
			System.out.println("Nimbus Up Time: " + nimbusUpTime);
			System.out.println("Number of Supervisors: "
					+ summary.get_supervisors_size());
			System.out.println("Number of Topologies: "
					+ summary.get_topologies_size());
		} catch (TTransportException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}

	}

	public Double getSpoutLatency() {
		// Nimbus config parameter-values
		List<Double> spoutCompleteLatencies = new ArrayList<Double>();
		try {
			
			Iterator<TopologySummary> topologiesIterator = summary
					.get_topologies_iterator();
			while (topologiesIterator.hasNext()) {
				TopologySummary topology = topologiesIterator.next();

				// Spouts (All time)
				TopologyInfo topology_info = client.getTopologyInfo(topology
						.get_id());
				Iterator<ExecutorSummary> executorStatusItr = topology_info
						.get_executors_iterator();
				while (executorStatusItr.hasNext()) {
					// get the executor
					ExecutorSummary executor_summary = executorStatusItr.next();
					ExecutorStats execStats = executor_summary.get_stats();
					ExecutorSpecificStats execSpecStats = execStats
							.get_specific();
					String componentId = executor_summary.get_component_id();
					// if the executor is a spout
					if (execSpecStats.is_set_spout()) {
						SpoutStats spoutStats = execSpecStats.get_spout();
						double latency = getStatValueFromMap(spoutStats.get_complete_ms_avg(), ":all-time");
						spoutCompleteLatencies.add(latency);
					}
				}

			}
			
			Double sumOfLatencies  = 0.0;
			for( Double temp: spoutCompleteLatencies) {
				sumOfLatencies += temp;
			}
			
			return sumOfLatencies / spoutCompleteLatencies.size();

		} catch (TTransportException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotAliveException e) {
			e.printStackTrace();
		}
		
		return 0.0;
	}

	/*
	 * Utility method to parse a Map<>
	 */
	public static Double getStatValueFromMap(Map<String, Map<String, Double>> map,
			String statName) {
		Double statValue = null;
		Map<String, Double> intermediateMap = map.get(statName);
		statValue = intermediateMap.get("default");
		return statValue;
	}

}
