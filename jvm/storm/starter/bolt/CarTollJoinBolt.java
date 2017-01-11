package storm.starter.bolt;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import storm.starter.util.TupleHelpers;
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
public class CarTollJoinBolt extends BaseRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector _collector;
	private Fields _custKeyFields;
	private Fields _outFields;
	private int _numSources;
	private BufferedWriter bw = null;
	
	RotatingMap<List<Object>, Map<GlobalStreamId, List<Tuple>>> _pending; // save
	// temp
	// data
	Map<String, GlobalStreamId> _fieldLocations;

	public CarTollJoinBolt(Fields outFields) {
		_outFields = outFields;
	}

	public CarTollJoinBolt() {
		_outFields = new Fields("carid", "toll", "xway", "dir", "seg");
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
			// Log.info("Received tick tuple, triggering emit of current window counts");
			// do something
		} else {
			List<Object> custKey = tuple.select(_custKeyFields);
			GlobalStreamId streamId = new GlobalStreamId(
					tuple.getSourceComponent(), tuple.getSourceStreamId());
			if (!_pending.containsKey(custKey)) {
				_pending.put(custKey,
						new HashMap<GlobalStreamId, List<Tuple>>());
			}
			Map<GlobalStreamId, List<Tuple>> parts = _pending.get(custKey);
			if (parts.containsKey(streamId)) {

				if (streamId.get_componentId().equals("wayjoinbolt")) {

					parts.get(streamId).remove(0);
					parts.get(streamId).add(tuple);
				} else {
					parts.get(streamId).add(tuple);
				}

			} else {
				List<Tuple> dataList = new ArrayList<Tuple>();
				dataList.add(tuple);
				parts.put(streamId, dataList);
			}
			
			_collector.ack(tuple);
			if (parts.size() == _numSources) {

				while (parts.get(_fieldLocations.get("carid")).size() > 0) {

					List<Object> joinResult = new ArrayList<Object>();
					List<Tuple> anchorTuples = new ArrayList<Tuple>();
					for (String outField : _outFields) {
						GlobalStreamId loc = _fieldLocations.get(outField);
						joinResult.add(parts.get(loc).get(0)
								.getValueByField(outField));
						anchorTuples.add(parts.get(loc).get(0));
					}
					//System.out.println("emit results: "+ joinResult.toString());
					//Log.info("emit results: " + joinResult.toString());
					try {
						bw.write(joinResult.toString() + "\n");
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					_collector.emit(joinResult);
//					for (Tuple part : anchorTuples) {
//						_collector.ack(part);
//					}
					parts.get(_fieldLocations.get("carid")).remove(0);

				}

			}
		}

	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector arg2) {
		// TODO Auto-generated method stub
		try {
			FileWriter fw = new FileWriter("/home/wamdm/wengzujian/lrbNfsData/LRBQueryResult.out");
			//FileWriter fw = new FileWriter("tpch/LRBData/LRBQueryResult.out");
			bw = new BufferedWriter(fw);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		_fieldLocations = new HashMap<String, GlobalStreamId>();
		_collector = arg2;
		_pending = new RotatingMap<List<Object>, Map<GlobalStreamId, List<Tuple>>>(
				new ExpireCallback());
		_numSources = context.getThisSources().size();
		Set<String> custKeyFields = null;
		for (GlobalStreamId source : context.getThisSources().keySet()) {
			Fields fields = context.getComponentOutputFields(
					source.get_componentId(), source.get_streamId());
			Set<String> setFields = new HashSet<String>(fields.toList());
			if (custKeyFields == null)
				custKeyFields = setFields;
			else
				custKeyFields.retainAll(setFields);

			for (String outfield : _outFields) {
				for (String sourcefield : fields) {
					if (outfield.equals(sourcefield)) {
						_fieldLocations.put(outfield, source);
					}
				}
			}
		}
		_custKeyFields = new Fields(new ArrayList<String>(custKeyFields));

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
			throw new RuntimeException("expired data error!!!");

		}
	}

}
