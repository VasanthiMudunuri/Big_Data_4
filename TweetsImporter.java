import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.parser.ParseException;

public class TweetsImporter extends Configured implements Tool {
	private static final String NAME = "Tweets Importer";

	static class TweetsMapper<K> extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
		private TweetsParser parser = new TweetsParser();

		public void map(LongWritable key, Text value, Context context) throws
		IOException, InterruptedException {
			try {
				parser.parse(value.toString());
			} catch (ParseException e) {
				e.printStackTrace();
			}
			byte[] rowKey = (parser.getUser__id()).getBytes();
			Put put = new Put(rowKey); //adding columns to the column family
			put.addColumn(TwitterQuery.TWEETS_COLUMNFAMILY, TwitterQuery.NAME_QUALIFIER, Bytes.toBytes(parser.getUser__name()));
			put.addColumn(TwitterQuery.TWEETS_COLUMNFAMILY, TwitterQuery.LOCATION_QUALIFIER, Bytes.toBytes(parser.getUser__location()));
			put.addColumn(TwitterQuery.TWEETS_COLUMNFAMILY, TwitterQuery.ID_QUALIFIER, Bytes.toBytes(parser.getId()));
			put.addColumn(TwitterQuery.TWEETS_COLUMNFAMILY, TwitterQuery.TIME_QUALIFIER, Bytes.toBytes(parser.getCreated_at()));
			put.addColumn(TwitterQuery.TWEETS_COLUMNFAMILY, TwitterQuery.TIMESTAMP_QUALIFIER, Bytes.toBytes(parser.getTimestamp_ms()));
			put.addColumn(TwitterQuery.TWEETS_COLUMNFAMILY, TwitterQuery.TEXT_QUALIFIER, Bytes.toBytes(parser.getText()));
			put.addColumn(TwitterQuery.TWEETS_COLUMNFAMILY, TwitterQuery.FAVORITE_QUALIFIER, Bytes.toBytes(parser.getFavorited()));
			put.addColumn(TwitterQuery.TWEETS_COLUMNFAMILY, TwitterQuery.FAVORITECOUNT_QUALIFIER, Bytes.toBytes(parser.getFavorite_count()));
			put.addColumn(TwitterQuery.TWEETS_COLUMNFAMILY, TwitterQuery.RETWEETED_QUALIFIER, Bytes.toBytes(parser.getRetweeted()));
			context.write(new ImmutableBytesWritable(rowKey), put);
		}
	}
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("Usage: TweetsImporter <input> <table-name>");
			return -1;
		}
		String inputPath = args[0];
		String hbaseTable = args[1];

		/*Check whether input path is null/empty */
		if (inputPath.length() == 0 || inputPath == null){
			System.out.println("Input Path cannot be null/empty");
			return -1;
		} 
		/*Check whether HTable name is null/empty */
		if (hbaseTable.length() == 0 || hbaseTable == null){
			System.out.println("HTable name cannot be null/empty");
			return -1;
		}
		Job job = Job.getInstance(getConf(), TweetsImporter.getName());
		job.setJarByClass(getClass());
		job.setMapperClass(TweetsMapper.class);
		TableMapReduceUtil.initTableReducerJob(hbaseTable, null, job);
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(HBaseConfiguration.create(), new TweetsImporter(), args);
		System.exit(exitCode);
	}

	public static String getName() {
		return NAME;
	}
}
