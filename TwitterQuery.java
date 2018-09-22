import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TwitterQuery extends Configured implements Tool {
  static final byte[] TWEETS_COLUMNFAMILY = Bytes.toBytes("Tweets");
  static final byte[] USERID_QUALIFIER = Bytes.toBytes("UserID");
  static final byte[] NAME_QUALIFIER = Bytes.toBytes("UserName");
  static final byte[] LOCATION_QUALIFIER = Bytes.toBytes("UserLocation");
  static final byte[] TIME_QUALIFIER = Bytes.toBytes("Time");
  static final byte[] TIMESTAMP_QUALIFIER = Bytes.toBytes("Timestamp");
  static final byte[] FAVORITE_QUALIFIER = Bytes.toBytes("favorite");
  static final byte[] FAVORITECOUNT_QUALIFIER = Bytes.toBytes("favorite count");
  static final byte[] RETWEETED_QUALIFIER = Bytes.toBytes("retweeted");
  static final byte[] ID_QUALIFIER = Bytes.toBytes("id");
  static final byte[] TEXT_QUALIFIER = Bytes.toBytes("text");
  
  public NavigableMap<Long, String> getUserTweets(Table table,
      String userId,String Count) throws IOException {
    byte[] startRow = userId.getBytes();
    NavigableMap<Long, String> resultMap = new TreeMap<Long, String>();
    Scan scan = new Scan(startRow);
    ResultScanner scanner = table.getScanner(scan);
    try {
      Result res;
      int count = 0;
      while ((res = scanner.next()) != null && count++ < Integer.parseInt(Count)) {
        byte[] value = res.getValue(TWEETS_COLUMNFAMILY, TEXT_QUALIFIER);
        byte[] time=res.getValue(TWEETS_COLUMNFAMILY,TIME_QUALIFIER);
        Long timevalue=Bytes.toLong(time);
        String text = Bytes.toString(value);
        resultMap.put(timevalue, text);
      }
    } finally {
      scanner.close();
    }
    return resultMap;
  }

  public int run(String[] args) throws IOException {
    if (args.length != 3) {
      System.err.println("Usage: TwitterQuery <table-name> <user_id>");
      return -1;
    }

    Configuration config = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(config);
    try {
      TableName tableName = TableName.valueOf(args[0]);
      Table table = connection.getTable(tableName);
      try {
        NavigableMap<Long, String> tweets =
            getUserTweets(table, args[1], args[2]);
        for (Map.Entry<Long, String> observation : tweets.entrySet()) { //printing the result to console
          System.out.printf("%1$tF %1$tR\t%2$s\n", observation.getKey(),
              observation.getValue());
        }
        return 0;
      } finally {
        table.close();
      }
    } finally {
      connection.close();
    }
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(HBaseConfiguration.create(),
        new TwitterQuery(), args);
    System.exit(exitCode);
  }
}
