package com.movie.rating;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgRatingOnDate {
   public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
      Configuration conf = new Configuration();
      Job job = new Job(conf, "Avg Rating on Date");
      job.setJarByClass(AvgRatingOnDate.class);
      job.setMapperClass(AvgRatingOnDate.RatingMapper.class);
      job.setCombinerClass(AvgRatingOnDate.AverageReducer.class);
      job.setReducerClass(AvgRatingOnDate.AverageReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(DoubleWritable.class);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
   }

   public static class AverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
      private DoubleWritable result = new DoubleWritable();

      public void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
         double sum = 0.0D;
         int count = 0;

         for(Iterator itVar = values.iterator(); itVar.hasNext(); ++count) {
            DoubleWritable val = (DoubleWritable)itVar.next();
            sum += val.get();
         }

         this.result.set(sum / (double)count);
         context.write(key, this.result);
      }
   }

   public static class RatingMapper extends Mapper<Object, Text, Text, DoubleWritable> {
      private DoubleWritable rating = new DoubleWritable();
      private Text date = new Text();

      public void map(Object key, Text value, Mapper<Object, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
         String line = value.toString();
         if (!line.contains(":")) {
            String[] line_values = line.split(",");
            this.date.set(line_values[2]);
            this.rating.set(Double.parseDouble(line_values[1]));
            context.write(this.date, this.rating);
         }

      }
   }
}