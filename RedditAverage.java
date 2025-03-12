import java.io.IOException;
import org.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class RedditAverage {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, LongPairWritable> {

        private Text subreddit = new Text();
        private static long one_comment = 1;
        private LongPairWritable pair = new LongPairWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
          JSONObject record = new JSONObject(value.toString());
          String subreddit_name = record.getString("subreddit");
          long score = record.getLong("score");

          pair.set(one_comment, score);

          subreddit.set(subreddit_name);
          context.write(subreddit, pair);
        }
      }

      public static class AverageScoreReducer extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
        private DoubleWritable average = new DoubleWritable();

        public void reduce(Text key, Iterable<LongPairWritable> values,
            Context context) throws IOException, InterruptedException {

          double comment_count = 0;
          double score = 0;
          for (LongPairWritable value : values) {
            comment_count += value.get_0();
            score += value.get_1();
          }
          average.set(score / comment_count);
          context.write(key, average);
        }
      }

      public static class RedditCombiner extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {
        private LongPairWritable result = new LongPairWritable();

        public void reduce(Text key, Iterable<LongPairWritable> values,
            Context context) throws IOException, InterruptedException {

          long comment_count = 0;
          long score = 0;
          for (LongPairWritable value : values) {
            comment_count += value.get_0();
            score += value.get_1();
          }
          result.set(comment_count, score);
          context.write(key, result);
        }
      }

      public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        args = optionParser.getRemainingArgs();

        Job job = Job.getInstance(conf, conf.get("job.name", "Reddit Average"));
        job.setJarByClass(RedditAverage.class);
        // Mapper
        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongPairWritable.class);

        if (conf.getBoolean("combiner", true)){
          System.out.println("Using Combiner");
          job.setCombinerClass(RedditCombiner.class);
        }
        else {
          System.out.println("Not using Combiner");
        }

        // Reducer
        job.setReducerClass(AverageScoreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // job.setNumReduceTasks(3);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
      }
    }
    