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

public class AvgUserRating {
   public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
      Configuration conf = new Configuration();
      Job job = new Job(conf, "AvgUser Rating");
      job.setJarByClass(AvgUserRating.class);
      job.setMapperClass(AvgUserRating.RatingMapper.class);
      job.setCombinerClass(AvgUserRating.AverageReducer.class);
      job.setReducerClass(AvgUserRating.AverageReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(DoubleWritable.class);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
   //Reducer
   public static class AverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
      private DoubleWritable result = new DoubleWritable();

      public void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
         double sum = 0.0D;
         int count = 0;

         for(Iterator itVar = values.iterator(); itVar.hasNext(); ++count) {
            DoubleWritable val = (DoubleWritable)itVar.next();
            sum += val.get();
         }
         //Average calculation
         this.result.set(sum / (double)count);
         context.write(key, this.result);
      }
   }
   // Mapper 
   public static class RatingMapper extends Mapper<Object, Text, Text, DoubleWritable> {
      private DoubleWritable rating = new DoubleWritable();
      private Text customer = new Text();

      public void map(Object key, Text value, Mapper<Object, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
         String line = value.toString();
         //Removing movie ID
         if (!line.contains(":")) {
            String[] line_values = line.split(",");
            this.customer.set(line_values[0]);
            this.rating.set(Double.parseDouble(line_values[1]));
            context.write(this.customer, this.rating);
         }

      }
   }
}