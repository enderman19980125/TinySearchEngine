package pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
    private static double d = 0.85;
    private static String inputPath;
    private static String outputPath;
    private static int iter = 0;

    private static void parseArgs(String[] args) throws ArrayIndexOutOfBoundsException {
        inputPath = args[0];
        outputPath = args[1];

        for (int i = 2; i < args.length; i++) {
            if ("-d".equals(args[i])) {
                d = Double.parseDouble(args[++i]);
                continue;
            }
            if ("-t".equals(args[i])) {
                iter = Integer.parseInt(args[++i]);
            }
            throw new ArrayIndexOutOfBoundsException(String.format("The argument \"%s\" is invalid.", args[i]));
        }
    }

    private static Job getJobInstance() throws IOException {
        Job job = Job.getInstance();
        job.setJobName("PageRank");
        job.setJarByClass(Main.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        String inPath = iter == 0 ? inputPath : String.format("%s-%d", outputPath, iter - 1);
        String outPath = String.format("%s-%d", outputPath, iter);
        FileInputFormat.setInputPaths(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        Configuration configuration = job.getConfiguration();
        configuration.setFloat("d", (float) d);

        return job;
    }

    private static void buildGraph() throws IOException, InterruptedException, ClassNotFoundException {
        Job job = getJobInstance();
        job.setMapperClass(pagerank.BuildGraph.BuildGraphMapper.class);
        job.setReducerClass(pagerank.BuildGraph.BuildGraphReducer.class);
        job.setInputFormatClass(pagerank.CompleteFileInputFormat.class);
        job.waitForCompletion(true);
    }

    private static void pageRank() throws IOException, InterruptedException, ClassNotFoundException {
        Job job = getJobInstance();
        job.setMapperClass(pagerank.PageRank.PageRankMapper.class);
        job.setReducerClass(pagerank.PageRank.PageRankReducer.class);
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        parseArgs(args);
        buildGraph();

        while (iter < 10) {
            iter++;
            pageRank();
        }
    }
}