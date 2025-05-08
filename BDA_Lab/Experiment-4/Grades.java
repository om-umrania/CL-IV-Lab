import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Grades {

  public static class GradeMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] parts = value.toString().split(",");
      String studentId = parts[0];
      float total = 0;
      for (int i = 1; i < parts.length; i++) {
        total += Float.parseFloat(parts[i]);
      }
      float avg = total / (parts.length - 1);
      context.write(new Text(studentId), new FloatWritable(avg));
    }
  }

  public static class GradeReducer extends Reducer<Text, FloatWritable, Text, Text> {
    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
      float avg = 0;
      for (FloatWritable val : values) {
        avg = val.get(); // only one per key from mapper
      }
      String grade;
      if (avg >= 80) {
        grade = "A";
      } else if (avg >= 60) {
        grade = "B";
      } else if (avg >= 40) {
        grade = "C";
      } else {
        grade = "D";
      }
      context.write(key, new Text(grade));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Student Grades");
    job.setJarByClass(Grades.class);
    job.setMapperClass(GradeMapper.class);
    job.setReducerClass(GradeReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
