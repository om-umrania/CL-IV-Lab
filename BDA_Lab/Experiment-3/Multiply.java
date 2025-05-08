import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Multiply {

  public static class MatrixMapper extends Mapper<LongWritable, Text, Text, Text> {
    private String fileTag;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      String filePath = context.getInputSplit().toString();
      fileTag = filePath.contains("M.txt") ? "M" : "N";
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] tokens = value.toString().split(",");
      if (fileTag.equals("M")) {
        String i = tokens[0];
        String j = tokens[1];
        String val = tokens[2];
        for (int k = 0; k < 2; k++) { // 2 = N's column count
          context.write(new Text(i + "," + k), new Text("M," + j + "," + val));
        }
      } else {
        String j = tokens[0];
        String k = tokens[1];
        String val = tokens[2];
        for (int i = 0; i < 2; i++) { // 2 = M's row count
          context.write(new Text(i + "," + k), new Text("N," + j + "," + val));
        }
      }
    }
  }

  public static class MatrixReducer extends Reducer<Text, Text, Text, IntWritable> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      Map<String, Integer> mapM = new HashMap<>();
      Map<String, Integer> mapN = new HashMap<>();

      for (Text val : values) {
        String[] tokens = val.toString().split(",");
        if (tokens[0].equals("M")) {
          mapM.put(tokens[1], Integer.parseInt(tokens[2]));
        } else {
          mapN.put(tokens[1], Integer.parseInt(tokens[2]));
        }
      }

      int result = 0;
      for (String j : mapM.keySet()) {
        if (mapN.containsKey(j)) {
          result += mapM.get(j) * mapN.get(j);
        }
      }

      context.write(key, new IntWritable(result));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Matrix Multiply");

    job.setJarByClass(Multiply.class);
    job.setMapperClass(MatrixMapper.class);
    job.setReducerClass(MatrixReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}