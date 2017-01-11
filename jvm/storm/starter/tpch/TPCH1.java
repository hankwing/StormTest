package storm.starter.tpch;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	lineitem
where
	l_shipdate <= date '1998-12-01' - interval ':1' day (3)
group by
	l_returnflag,
	l_linestatus
 * @author hankwing
 *
 */
public class TPCH1 {
	
	public static class SplitBolt extends BaseRichBolt {

		OutputCollector _collector = null;

		@Override
		public void execute(Tuple arg0) {
			// TODO Auto-generated method stub

			String[] strings = arg0.getString(0).split("\\|");
			_collector.ack(arg0);
			_collector.emit(new Values(Float.valueOf(strings[4]), Float
					.valueOf(strings[5]), Float.valueOf(strings[6]), Float
					.valueOf(strings[7]), strings[8], strings[9], strings[10]));

		}

		@Override
		public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
			// TODO Auto-generated method stub
			_collector = arg2;
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer arg0) {
			// TODO Auto-generated method stub
			arg0.declare(new Fields("quantity", "extendedprice", "discount",
					"tax", "returnflag", "linestatus", "shipdate"));
		}

	}
	
	public static class Tpch1Bolt extends BaseRichBolt {

		OutputCollector _collector = null;
		Float sum_qty = 0f;
		Float sum_base_price = 0f;
		Float sum_disc_price = 0f;
		Float sum_charge = 0f;
		Float avg_qty = 0f;
		Float avg_price = 0f;
		Float avg_disc = 0f;
		long count_order = 0;

		@Override
		public void execute(Tuple arg0) {
			// TODO Auto-generated method stub
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
			Date shipDate;
			try {
				shipDate = formatter.parse(arg0.getString(6));
				Date comparedDate = formatter.parse("1998-09-03");
				if (shipDate.compareTo(comparedDate) < 0) {
					sum_qty += arg0.getFloat(0);
					sum_base_price += arg0.getFloat(1);
					sum_disc_price += (arg0.getFloat(1) * (1 - arg0.getFloat(2)));
					sum_charge += (arg0.getFloat(1) * (1 - arg0.getFloat(2)) * (1 + arg0
							.getFloat(3)));
					avg_qty = (avg_qty + arg0.getFloat(0)) / 2;
					avg_price = (avg_price + arg0.getFloat(1)) / 2;
					avg_disc = (avg_disc + arg0.getFloat(2)) / 2;
					count_order++;
					Values result = new Values(arg0.getString(4),
							arg0.getString(5), sum_qty, sum_base_price,
							sum_disc_price, sum_charge, avg_qty, avg_price,
							avg_disc, count_order);
					_collector.emit(result);
					// LOG.info("result:" + result.toString());
				}
				_collector.ack(arg0);

			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		@Override
		public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
			// TODO Auto-generated method stub
			_collector = arg2;
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer arg0) {
			// TODO Auto-generated method stub
			arg0.declare(new Fields("returnflag", "linestatus", "sum_qty",
					"sum_base_price", "sum_disc_price", "sum_charge",
					"avg_qty", "avg_price", "avg_disc", "count_order"));
		}

	}
	
}
