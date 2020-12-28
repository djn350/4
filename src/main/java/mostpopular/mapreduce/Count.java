package mostpopular.mapreduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Count {
    /**
     * @param UserLog  user_log_file_name
     * @param UserInfo user_info_file_name
     * @param Type     Type=1 商品 Type=2 商店
     */
    public static boolean run(String UserLog, String UserInfo, String out, Integer Type)
            throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("Count run out: " + out);
        Configuration conf = new Configuration();
        conf.set("UserInfo", UserInfo);
        conf.set("Type", Type.toString());
        Job job = Job.getInstance(conf, "Count" + Type);

        job.setJarByClass(Count.class);
        job.setMapperClass(CountMapper.class);
        job.setReducerClass(CountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(UserLog));
        FileOutputFormat.setOutputPath(job, new Path(out));
        return job.waitForCompletion(true);
    }

    public static class CountMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        Text KeyOut = new Text();
        IntWritable ValueOut = new IntWritable();
        private Map<String, String> UserInfoMap;
        private String Type;

        @Override
        protected void setup(Context context) throws IOException {
            //建立索引表
            UserInfoMap = new HashMap<>();
            String UserInfoFileName = context.getConfiguration().get("UserInfo");
            BufferedReader bufferedReader = new BufferedReader(new FileReader(UserInfoFileName));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                List<String> vals = Arrays.asList(line.split(",", -1));
                UserInfoMap.put(vals.get(0), vals.get(1));
            }
            bufferedReader.close();
            //获取商家还是商品的
            Type = context.getConfiguration().get("Type");
        }

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] userLog = value.toString().split(",", -1);
            boolean flag = true;
            if (!userLog[6].equals("0")) flag = false;
            if (Type.equals("2")) {
                String userAge = UserInfoMap.get(userLog[0]);
                if (userAge.compareTo("4") >= 0 || userAge.compareTo("0") <= 0)
                    flag = false;
            }
            if (flag) ValueOut.set(1);
            else ValueOut.set(0);
            if (Type.equals("1"))
                KeyOut.set(userLog[1]);
            else if (Type.equals("2"))
                KeyOut.set(userLog[3]);
            context.write(KeyOut, ValueOut);
        }
    }

    public static class CountReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            IntWritable result = new IntWritable();
            int sum = 0;
            //遍历迭代器values，得到同一key的所有value
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            //产生输出对<key,value>
            context.write(key, result);
        }
    }
}
