import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountImproved {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, LongWritable>{

    private final static LongWritable one = new LongWritable(1);
    private Text word = new Text();
    private final static Pattern word_sep = Pattern.compile("[\\p{Punct}\\s]+");

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      String[] tokens = word_sep.split(value.toString().toLowerCase());

      for (String token : tokens) {
        if (token.isEmpty()){
          continue;
        }
        word.set(token);
        context.write(word, one);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
    args = optionParser.getRemainingArgs();

    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCountImproved.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(LongSumReducer.class);
    job.setReducerClass(LongSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    // job.setNumReduceTasks(3);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}