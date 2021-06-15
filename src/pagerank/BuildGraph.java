package pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class BuildGraph {
    public static class BuildGraphMapper extends Mapper<NullWritable, BytesWritable, Text, Text> {
        public void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
            String content = new String(value.getBytes()).replaceAll("\0", "");
            ContentFileInfo contentFileInfo = new ContentFileInfo(content);

            Text empty = new Text();
            Text fromURL = new Text(contentFileInfo.getURL());

            for (String nextURL : contentFileInfo.getNextURLs()) {
                Text toURL = new Text(nextURL);
                context.write(fromURL, toURL);
                context.write(toURL, empty);
            }
        }
    }

    public static class BuildGraphReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("1.00");

            int numPages = 0;
            for (Text value : values) {
                if (value.getLength() > 0) {
                    numPages++;
                    stringBuilder.append("\t").append(value);
                }
            }

            if (numPages == 0) stringBuilder.append("\t");
            Text toURLs = new Text(stringBuilder.toString());
            context.write(key, toURLs);
        }
    }
}