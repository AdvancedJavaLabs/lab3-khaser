import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Main {

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();

        Job job1 = Job.getInstance(new Configuration(), "Category Revenue Quantity");
        job1.setJarByClass(Main.class);
        job1.setMapperClass(RevenueQuantityMapper.class);
        job1.setReducerClass(RevenueQuantityReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        int numReducersJob = Integer.parseInt(args[3]);
        job1.setNumReduceTasks(numReducersJob);
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(new Configuration(), "Sort Categories");
        job2.setJarByClass(Main.class);
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setSortComparatorClass(DescendingTextComparator.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.setNumReduceTasks(1);
        job2.waitForCompletion(true);

        long end = System.currentTimeMillis();

        System.out.println("Total time: " + (end - start));

        try (BufferedWriter writer = new BufferedWriter(new FileWriter("logs/execution_time_output_" + numReducersJob + ".txt"))) {
            String text = "Total execution time for " + numReducersJob + " reducers: " + (end - start) + " ms";
            writer.write(text);
            writer.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
