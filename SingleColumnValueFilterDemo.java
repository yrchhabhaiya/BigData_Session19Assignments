package filter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SkipFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

@SuppressWarnings("unused")
public class SingleColumnValueFilterDemo {
	@SuppressWarnings({ "resource", "deprecation" })
	public static void main(String[] args) throws IOException, InterruptedException {
		Configuration conf = HBaseConfiguration.create();
		
		String tableName = "customer";
		byte[] columnFamily = Bytes.toBytes("details");
		byte[] coulmnQualifier = Bytes.toBytes("location");
		byte[] columnValue = Bytes.toBytes("AUS");
		BinaryComparator bc = new BinaryComparator(columnValue);
		
		System.out.println("Creating HTable instance to 'customer'...");
		HTable table = new HTable(conf, tableName);
		
		System.out.println("Creating scan object...");
		Scan scan = new Scan();
		
		System.out.println("Narrowing down the result to details column family...");
		scan.addFamily(columnFamily);
		
		System.out.println("Adding single column value filter on scan object...");
		
		SingleColumnValueFilter scvf = new SingleColumnValueFilter(columnFamily, coulmnQualifier, CompareOp.EQUAL, bc);
		
		scan.setFilter(scvf);

		System.out.println("Getting a result scanner object...");
		ResultScanner rs = table.getScanner(scan);
		
		for (Result r : rs) {
			for(KeyValue kv : r.raw()){
		           System.out.print(new String(kv.getRow()) + " ");
		           System.out.print(new String(kv.getFamily()) + ":");
		           System.out.print(new String(kv.getQualifier()) + " ");
		           //System.out.print(kv.getTimestamp() + " ");
		           System.out.println(new String(kv.getValue()));
	        }
		}
		
		System.out.println("Closing Scanner instance...");
		rs.close();
	}
}
