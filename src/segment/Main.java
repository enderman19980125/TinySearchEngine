package segment;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
    private static String inputPath;
    private static String outputPath;

    private static void parseArgs(String[] args) throws ArrayIndexOutOfBoundsException {
        inputPath = args[0];
        outputPath = args[1];
    }

    private static Job getJobInstance() throws IOException {
        Job job = Job.getInstance();
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setJobName("Segment");
        job.setJarByClass(Main.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job;
    }

    private static void segment() throws IOException, InterruptedException, ClassNotFoundException {
        Job job = getJobInstance();
        job.setMapperClass(Segment.SegmentMapper.class);
        job.setReducerClass(Segment.SegmentReducer.class);
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        parseArgs(args);
        segment();
    }
}