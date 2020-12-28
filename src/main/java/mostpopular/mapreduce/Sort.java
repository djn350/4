package mostpopular.mapreduce;

import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;

public class Sort {
    private static int k;

    private static class SortMapper
            extends Mapper<LongWritable, Text, IntWritable, Text> {
        IntWritable KeyOut = new IntWritable();
        Text ValueOut = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");
            KeyOut.set(Integer.parseInt(data[1]));
            ValueOut.set(data[0]);
            context.write(KeyOut, ValueOut);
        }

    }

    private static class SortReducer
            extends Reducer<IntWritable, Text, Text, IntWritable> {

        PriorityQueue<Pair<String, Integer>> minHeap = new PriorityQueue<>(Comparator.comparingInt(Pair::getValue));

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text text : values) {
                context.write(text, key);
                minHeap.add(new Pair<>(text.toString(), key.get()));
                if (minHeap.size() > k) {
                    minHeap.poll();
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            String path = context.getConfiguration().get("output");
            System.out.println(path);
            MultipleOutputs<Text, IntWritable> mos = new MultipleOutputs<>(context);
            List<Pair<String, Integer>> pairList = new ArrayList<>(minHeap);
            pairList.sort(Collections.reverseOrder(Comparator.comparingInt(Pair::getValue)));
            int i = 0;
            for (Pair<String, Integer> t : pairList) {
                mos.write("topK",
                        new Text(t.getKey()),
                        new IntWritable(t.getValue()),
                        path);
            }
            mos.close();
        }
    }

    public static void run(String in, String out, String topKout, Integer Topk)
            throws IOException, ClassNotFoundException, InterruptedException {
        k = Topk;
        System.out.println("run in: " + in + " out: " + out + " topKout: " + topKout + " TopK: " + Topk);
        Configuration conf = new Configuration();
        conf.set("output", topKout);
        Job job = Job.getInstance(conf, "SortKey" + topKout);

        job.setJarByClass(Sort.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        MultipleOutputs.addNamedOutput(job, "topK", TextOutputFormat.class, Text.class, IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.waitForCompletion(true);
    }
}
