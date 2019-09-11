/**
 * @author: Tianfang Ni, Mengzhi Cao
 * @create: 2019-02-04
 * @program: Friend Analytics
 * @Description: CS585 PROJECT 1 "Friend Analytics"
 **/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

/**
 * @program: CS585 Project1
 * @Description: cs585 TaskH, report the people  who have more than average friends
 * @author: Tianfang Ni, Mengzhi Cao
 * @create: 2019-02-04
 **/
public class TaskH {
    public static class TaskHMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        /**
        * @Description: Mapper for Friends table, set user id as key, and each friend as one value
        * @param: [key, value, context]
        * @return: void
        */
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] tuple = value.toString().split(",");
            word.set(tuple[1]);
            context.write(word, one);

        }
    }

    public static class TaskHCombiner
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        /**
        * @Description: combine the key value pair for each mapper
        * @param: [key, values, context]
        * @return: void
        */
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class TaskHReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private double total = 0.0;
        private HashMap<String, Integer> map = new HashMap<>();
        /**
        * @Description: use one reducer and use hashMap to record how many friends each user have，and in cleanup phase,
         * we only output the number over the average user
        * @param: [key, values, context]
        * @return: void
        */
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            total += sum;
            if (!map.containsKey(key.toString()))
                map.put(key.toString(), sum);


        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            total = total/map.size();
            for(String key : map.keySet()){
                if(map.get(key) > total)
                    context.write(new Text(key), new IntWritable(map.get(key)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();

        args = new String[2];
        args[0] = "./data/Friendstest.txt";
        args[1] = "./outputH";

        Job job = Job.getInstance(conf, "TaskH");
        job.setJarByClass(TaskH.class);

        job.setMapperClass(TaskHMapper.class);
        job.setCombinerClass(TaskHCombiner.class);
        job.setReducerClass(TaskHReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);



    }
}
