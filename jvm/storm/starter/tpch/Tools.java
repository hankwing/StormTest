package storm.starter.tpch;

import java.lang.management.ManagementFactory;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.hyperic.sigar.CpuPerc;
import org.hyperic.sigar.Mem;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

/**
 * using sigar to get system info
 * @author hankwing
 *
 */
public class Tools {
	
	public Sigar sigar = null;
	public CpuPerc cpuPerc = null;
	Mem mem = null;
	
	public Tools() {

		sigar = new Sigar();
	}
	
	public float getCpuUsage() {
		try {
			cpuPerc = sigar.getCpuPerc();
		} catch (SigarException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return (float) (cpuPerc.getSys() + cpuPerc.getUser());
	}
	
	public long getMemoryUsage() {
		try {
			mem = sigar.getMem();
		} catch (SigarException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return mem.getActualUsed();
	}

	public static double getProcessCpuLoad() {

		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		ObjectName name;
		Double value = 0.0;
		try {
			name = ObjectName.getInstance("java.lang:type=OperatingSystem");
			AttributeList list = mbs.getAttributes(name,
					new String[] { "ProcessCpuLoad" });

			if (list.isEmpty())
				return Double.NaN;

			Attribute att = (Attribute) list.get(0);
			value = (Double) att.getValue();

			// usually takes a couple of seconds before we get real values
			if (value == -1.0)
				return Double.NaN;
			// returns a percentage value with 1 decimal point precision
			
		} catch (MalformedObjectNameException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NullPointerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstanceNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ReflectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return ((int) (value * 1000) / 10.0);

	}
}
