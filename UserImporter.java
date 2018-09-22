import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UserImporter extends Configured implements Tool{
	@SuppressWarnings({ "deprecation" })
	public int run(String[] args) throws IOException {
		/*Check for the arguments*/
		if (args.length < 3 || args.length > 5) {
			System.err.println("Usage: UserImporter <file-system> <input> <table-name> <hdfs-master-url>");
			System.err.println(args.length);
			return -1;
		}

		String fileSystem = args[0];
		String inputPath = args[1];
		String hbaseTable = args[2];
		boolean isHdfsPath = args.length ==4? true:false ;
		String hdfsMaster = null;

		/*Check whether file system is null/empty */
		if (fileSystem.length() == 0 || fileSystem == null){
			System.err.println("File System cannot be null/empty");
			return -1;
		}else if (fileSystem.equalsIgnoreCase("local") || fileSystem.equalsIgnoreCase("hdfs")){
			System.out.println(fileSystem.toUpperCase() + " File system is supported");
		}else{
			System.err.println("Only Local/HDFS file system is supported"+fileSystem);
			return -1;
		}
		/*Check whether input path is null/empty */
		if (inputPath.length() == 0 || inputPath == null){
			System.err.println("Input Path cannot be null/empty");
			return -1;
		}
		/*Check whether HTable name is null/empty */
		if (hbaseTable.length() == 0 || hbaseTable == null){
			System.err.println("HTable name cannot be null/empty");
			return -1;
		}
		/*If file is being read from HDFS check whether HDFS Master URL name is null/empty */
		if (isHdfsPath){
			if(args[3].length() !=0 && args[3] != null){
				hdfsMaster = args[3];
			}else{
				System.err.println("HDFS Master URL cannot be null/empty");
				return -1;
			}
		}
		/*System.out.println(args[0]+args[1]+args[2]+isHdfsPath+hdfsMaster);
 * 	    System.exit(1);*/
		Configuration config = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(config);
		try {
			/*Get an object for the table we created*/
			TableName tableName = TableName.valueOf(hbaseTable);
			Table table = connection.getTable(tableName);
			try {
				UserMetadata metadata = new UserMetadata();
				metadata.initialize(inputPath, isHdfsPath, hdfsMaster);
				Map<String, List<String>> UserMap = metadata.getUserMap();

				for (Map.Entry<String, List<String>> entry : UserMap.entrySet()) {
					Put put = new Put(Bytes.toBytes(entry.getKey()));
					put.add(TwitterUserQuery.USERINFO_COLUMNFAMILY,
							TwitterUserQuery.NAME_QUALIFIER, Bytes.toBytes(entry.getValue().get(0)));
					put.add(TwitterUserQuery.USERINFO_COLUMNFAMILY,
							TwitterUserQuery.LOCATION_QUALIFIER, Bytes.toBytes(entry.getValue().get(1)));
					put.add(TwitterUserQuery.USERINFO_COLUMNFAMILY,
							TwitterUserQuery.LANGUAGE_QUALIFIER, Bytes.toBytes(entry.getValue().get(2)));
					put.add(TwitterUserQuery.USERINFO_COLUMNFAMILY,
							TwitterUserQuery.SCREENNAME_QUALIFIER, Bytes.toBytes(entry.getValue().get(3)));
					put.add(TwitterUserQuery.USERINFO_COLUMNFAMILY,
							TwitterUserQuery.FRIENDS_QUALIFIER, Bytes.toBytes(entry.getValue().get(4)));
					put.add(TwitterUserQuery.USERINFO_COLUMNFAMILY,
							TwitterUserQuery.FOLLOWERS_QUALIFIER, Bytes.toBytes(entry.getValue().get(5)));
					table.put(put);
				}
			} finally {
				table.close();
			}
		} finally {
			connection.close();
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(HBaseConfiguration.create(),
				new UserImporter(), args);
		System.exit(exitCode);
	}
}

