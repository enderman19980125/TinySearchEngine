package pagerank;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

public class PageRank {
    public static class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String fromURL = line.substring(0, line.indexOf("\t"));
            line = line.substring(line.indexOf("\t") + 1);
            double fromURLRankValue = Double.parseDouble(line.substring(0, line.indexOf("\t")));
            String toURLs = line.substring(line.indexOf("\t") + 1);

            context.write(new Text(fromURL), new Text("G" + toURLs));

            StringTokenizer stringTokenizer = new StringTokenizer(toURLs, "\t");
            int numToURLs = stringTokenizer.countTokens();

            while (stringTokenizer.hasMoreTokens()) {
                String toURL = stringTokenizer.nextToken();
                double toURLRankValue = fromURLRankValue / numToURLs;
                context.write(new Text(toURL), new Text("R" + toURLRankValue));
            }
        }
    }

    public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
        protected double d;

        protected void setup(Reducer.Context context) {
            Configuration configuration = context.getConfiguration();
            d = configuration.getFloat("d", (float) 0.85);
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String toURLs = "";
            double rankValue = 0.0;

            for (Text value : values) {
                char type = value.toString().charAt(0);
                String item = value.toString().substring(1);
                if ('G' == type) toURLs = item;
                if ('R' == type) rankValue += Double.parseDouble(item);
            }

            rankValue = d * rankValue + (1 - d);
            String value = String.format("%f\t%s", rankValue, toURLs);
            context.write(key, new Text(value));
        }
    }
}