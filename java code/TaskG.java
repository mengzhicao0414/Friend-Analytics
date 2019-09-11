/**
 * @author: Tianfang Ni, Mengzhi Cao
 * @create: 2019-02-04
 * @program: Friend Analytics
 * @Description: CS585 PROJECT 1 "Friend Analytics"
 **/


import org.apache.commons.io.input.BOMInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @program: CS585 Project1
 * @Description: cs585 TaskG, report the people  who are friend with someon but never access to them
 * @author: Tianfang Ni, Mengzhi Cao
 * @create: 2019-02-04
 **/
public class TaskG {
    public static class TaskGMapper1 extends Mapper<LongWritable, Text, Text, Text> {
        /**
         * @Description: Mapper for Friend table, set key as the userID, and value as the friendID
         * @param: [key, value, context]
         * @return: void
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tuple = value.toString().split(",");
            context.write(new Text(tuple[1]), new Text("friend," + tuple[2]));
        }
    }

    public static class TaskGMapper2 extends Mapper<LongWritable, Text, Text, Text> {
        /**
         * @Description: Mapper for access Time table, set the visitorID as key, and visited page ID as value
         * @param: [key, value, context]
         * @return: void
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tuple = value.toString().split(",");
            context.write(new Text(tuple[1]), new Text("accessLog," + tuple[2]));
        }
    }

    public static class TaskGReducer extends Reducer<Text, Text, Text, Text> {
        /**
         * @Description: use a hashmap save the distinct accessed page ID, and use arraylist to save the friend ID,
         * then for each element inthe list, check if there is a record in hashmap. If there is a missing one, report the key.
         * @param: [key, value, context]
         * @return: void
         */
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> map = new HashMap<>();
            ArrayList<String> list = new ArrayList<>();
            for (Text t : value) {
                String parts[] = t.toString().split(",");
                if (parts[0].equals("accessLog")) {
                    if (!map.containsKey(parts[1])) {
                        map.put(parts[1], 1);
                    } else if (parts[0].equals("friend")) {
                        list.add(parts[1]);
                    }
                }
            }
            boolean careOrNot = true;
            for (String friend : list) {
                if (!map.containsKey(friend))
                    careOrNot = false;
                break;
            }
            if (careOrNot)
                context.write(null, key);
        }

        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "TaskG");


            args = new String[3];
            args[0] = "./data/Friendstest.txt";
            args[1] = "./data/AccessTimetest.txt";
            args[2] = "./outputG";

            job.setJarByClass(TaskG.class);
            MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TaskGMapper1.class);
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TaskGMapper2.class);
            job.setReducerClass(TaskGReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
}
