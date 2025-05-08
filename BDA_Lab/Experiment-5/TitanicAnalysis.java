import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TitanicAnalysis {

  public static class TitanicMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text genderOrClass = new Text();
    private IntWritable age = new IntWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] fields = value.toString().split(",");

      if (fields.length > 6) {
        String survived = fields[1];
        String pclass = fields[2];
        String sex = fields[4];
        String ageStr = fields[5];

        // Task 1: Avg Age of Males/Females who died
        if (survived.equals("0") && ageStr.matches("\\d+")) {
          genderOrClass.set("DIED_" + sex.toUpperCase());
          age.set(Integer.parseInt(ageStr));
          context.write(genderOrClass, age);
        }

        // Task 2: Count of people survived per class
        if (survived.equals("1")) {
          genderOrClass.set("SURVIVED_CLASS_" + pclass);
          context.write(genderOrClass, new IntWritable(1));
        }
      }
    }
  }

  public static class TitanicReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0, count = 0;
      for (IntWritable val : values) {
        sum += val.get();
        count++;
      }

      // If key contains DIED_ prefix → calculate average age
      if (key.toString().startsWith("DIED_")) {
        int avg = sum / count;
        context.write(new Text("Average age of " + key.toString().split("_")[1].toLowerCase() + "s who died"), new IntWritable(avg));
      } else {
        // Otherwise, output the count of survivors in each class
        context.write(new Text("Survivors in Class " + key.toString().split("_")[2]), new IntWritable(sum));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Titanic Analysis");

    job.setJarByClass(TitanicAnalysis.class);
    job.setMapperClass(TitanicMapper.class);
    job.setReducerClass(TitanicReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}