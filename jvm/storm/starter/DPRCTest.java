package storm.starter;

import org.apache.thrift7.TException;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;

public class DPRCTest {
	
	public static void main(String[] args) {
		
		DRPCClient client = new DRPCClient("202.112.113.252", 3772);
		try {
			System.out.println(client.execute("reach", "cat dog the man"));
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DRPCExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
