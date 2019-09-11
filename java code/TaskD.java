/**
 * @author: Tianfang Ni, Mengzhi Cao
 * @create: 2019-02-04
 * @program: Friend Analytics
 * @Description: CS585 PROJECT 1 "Friend Analytics"
 **/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

import java.io.IOException;

public class TaskD {

    public static class TaskDMapper1 extends Mapper<LongWritable, Text, Text, Text> {
        /**
         * @Description: Mapper for Mypage, set key as ID, value as ID and user name
         * @param: [key, value, context]
         * @return: void
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] user = value.toString().split(",");
            context.write(new Text(user[0]), new Text("file1" + "," + user[0] + "," + user[1]));
        }
    }

    public static class TaskDMapper2 extends Mapper<LongWritable, Text, Text, Text> {
        /**
         * @Description: Mapper for Friends, set key as host ID, value as friend ID
         * @param: [key, value, context]
         * @return: void
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] friend = value.toString().split(",");
            context.write(new Text(friend[2]), new Text("file2"));
        }
    }

    public static class TaskDCombiner
            extends Reducer<Text, Text, Text, Text> {
        /**
         * @Description: only combine the <key,value> pair from mapper, sum the value for distinct ID,and report the ID, name and sum
         * @param: [key, values, context]
         * @return: void
         */
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            String name = "";
            String id = "";
            int sum = 0;
            for (Text t : values) {
                String[] parts = t.toString().split(",");
                if (parts[0].equals("file2")) {
                    sum++;
                } else if (parts[0].equals("file1")) {
                    id = parts[1];
                    name = parts[2];
                }
            }
            context.write(key, new Text(id + "," + name + "," + String.valueOf(sum)));
        }
    }

    public static class TaskDReducer extends Reducer<Text, Text, Text, Text> {
        /**
         * @Description: combine the key value pair from file 1 and file 2 combiner
         * @param: [key, value, context]
         * @return: void
         */
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            String name = "";
            int count = 0;
            String id = "";
            for (Text t : value) {
                String parts[] = t.toString().split(",");
                if(id.length() == 0)
                    id = parts[0];
                if(name.length() == 0)
                    name = parts[1];
                count += Integer.parseInt(parts[2]);
            }

            context.write(null, new Text(id + ", " + name + ", " + count));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskD");


        args = new String[3];
        args[0] = "./data/MyPagetest.txt";
        args[1] = "./data/Friendstest.txt";
        args[2] = "./outputD";

        job.setJarByClass(TaskD.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TaskDMapper1.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TaskDMapper2.class);
        job.setCombinerClass(TaskDCombiner.class);
        job.setReducerClass(TaskDReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}