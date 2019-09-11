/**
 * @author: Tianfang Ni, Mengzhi Cao
 * @create: 2019-02-04
 * @program: Friend Analytics
 * @Description: CS585 PROJECT 1 "Friend Analytics"
 **/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import java.io.IOException;
import java.util.HashMap;
/**
 * @program: CS585 Project1
 * @Description: cs585 TaskE, report the number of each user's friend and name
 * @author: Tianfang Ni, Mengzhi Cao
 * @create: 2019-02-04
 **/
public class TaskE {

    public static class TaskEMapper1 extends Mapper<LongWritable, Text, Text, Text> {
        /**
         * @Description: Mapper for MyPage, set key as userID, value as Id and userName
         * @param: [key, value, context]
         * @return: void
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] user = value.toString().split(",");
            context.write(new Text(user[0]), new Text("myPage," + user[0] + "," + user[1]));
        }
    }

    public static class TaskEMapper2 extends Mapper<LongWritable, Text, Text, Text> {
        /**
         * @Description: Mapper for AccessTime, set key as userID, value as pageID
         * @param: [key, value, context]
         * @return: void
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] transaction = value.toString().split(",");
            context.write(new Text(transaction[1]), new Text("accessLog,"  + transaction[2]));
        }
    }

    public static class TaskEReducer extends Reducer<Text, Text, Text, Text> {
        /**
         * @Description: Use HashMap to record the distinct page, and sum the total accesses
         * @param: [key, value, context]
         * @return: void
         */
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            String name = "";
            float total = 0.0f;
            int count = 0;
            String id = "";
            HashMap<String,Integer> map = new HashMap<>();
            for (Text t : value) {
                String parts[] = t.toString().split(",");
                if (parts[0].equals("accessLog")) {
                    count++;
                    if(!map.containsKey(parts[1]))
                        map.put(parts[1],1);
                } else if (parts[0].equals("myPage")) {
                    id = parts[1];
                    name = parts[2];
                }
            }

            context.write(null, new Text(id + ", " + name + ", " + count + ", " + map.size()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task E");

        args = new String[3];
        args[0] = "./data/MyPagetest.txt";
        args[1] = "./data/AccessTimetest.txt";
        args[2] = "./outputE";

        job.setJarByClass(TaskE.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TaskEMapper1.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TaskEMapper2.class);
        job.setReducerClass(TaskEReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}