import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TwitterUserQuery extends Configured implements Tool {
  static final byte[] USERINFO_COLUMNFAMILY = Bytes.toBytes("info");
  static final byte[] NAME_QUALIFIER = Bytes.toBytes("user__name");
  static final byte[] LOCATION_QUALIFIER = Bytes.toBytes("user__location");
  static final byte[] LANGUAGE_QUALIFIER=Bytes.toBytes("user__lang");
  static final byte[] SCREENNAME_QUALIFIER = Bytes.toBytes("user__Screen_Name");
  static final byte[] FRIENDS_QUALIFIER=Bytes.toBytes("user__friends_count");
  static final byte[] FOLLOWERS_QUALIFIER=Bytes.toBytes("user__followers_count");
  
  public Map<String, String> getUserInfo(Table table, String userId)
      throws IOException {
    Get get = new Get(Bytes.toBytes(userId));
    get.addFamily(USERINFO_COLUMNFAMILY);
    Result res = table.get(get);
    if (res == null) {
      return null;
    }
    Map<String, String> resultMap = new LinkedHashMap<String, String>();
    resultMap.put("user__name", getValue(res, USERINFO_COLUMNFAMILY,
        NAME_QUALIFIER));
    resultMap.put("user__location", getValue(res, USERINFO_COLUMNFAMILY,
        LOCATION_QUALIFIER));
    resultMap.put("user__lang", getValue(res, USERINFO_COLUMNFAMILY,
            LANGUAGE_QUALIFIER));
    resultMap.put("user__Screen_Name", getValue(res, USERINFO_COLUMNFAMILY,
            SCREENNAME_QUALIFIER));
    resultMap.put("user__friends_count", getValue(res, USERINFO_COLUMNFAMILY,
            FRIENDS_QUALIFIER));
    resultMap.put("user__followers_count", getValue(res, USERINFO_COLUMNFAMILY,
            FOLLOWERS_QUALIFIER));
    return resultMap;
  }

  private static String getValue(Result res, byte[] cf, byte[] qualifier) {
    byte[] value = res.getValue(cf, qualifier);
    return value == null? "": Bytes.toString(value);
  }

  public int run(String[] args) throws IOException {
    if (args.length != 2) {
      System.err.println("Usage: TwitterUserQuery <table-name> <user_id>");
      return -1;
    }

    Configuration config = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(config);
    try {
      TableName tableName = TableName.valueOf(args[0]);
      Table table = connection.getTable(tableName);
      try {
        Map<String, String> userInfo = getUserInfo(table, args[1]);
        if (userInfo == null) {
          System.err.printf("User ID %s not found.\n", args[1]);
          return -1;
        }
        for (Map.Entry<String, String> user : userInfo.entrySet()) { //printing the result
          System.out.printf("%s\t%s\n", user.getKey(), user.getValue());
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
        new TwitterUserQuery(), args);
    System.exit(exitCode);
  }
}
