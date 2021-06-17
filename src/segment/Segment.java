package segment;

import pagerank.ContentFileInfo;

import java.io.IOException;
import java.util.List;

import com.huaban.analysis.jieba.SegToken;
import com.huaban.analysis.jieba.JiebaSegmenter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Segment {
    public static class SegmentMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            JiebaSegmenter jiebaSegmenter = new JiebaSegmenter();
            ContentFileInfo contentFileInfo = new ContentFileInfo(value.toString());
            List<SegToken> segTitleList = jiebaSegmenter.process(contentFileInfo.getTitle(), JiebaSegmenter.SegMode.INDEX);
            List<SegToken> segBodyList = jiebaSegmenter.process(contentFileInfo.getBody(), JiebaSegmenter.SegMode.INDEX);

            for (SegToken segToken : segTitleList) {
                String word = segToken.word;
                String urlAtOffset = String.format("%s@@%d-%d", contentFileInfo.getURL(), segToken.startOffset, segToken.endOffset);
                context.write(new Text(word), new Text(urlAtOffset));
            }

            for (SegToken segToken : segBodyList) {
                String word = segToken.word;
                String urlAtOffset = String.format("%s@%d-%d", contentFileInfo.getURL(), segToken.startOffset, segToken.endOffset);
                context.write(new Text(word), new Text(urlAtOffset));
            }
        }
    }

    public static class SegmentReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder stringBuilder = new StringBuilder();
            boolean isFirst = true;
            int num = 0;

            for (Text value : values) {
                if (value.getLength() > 0) {
                    if (isFirst) {
                        isFirst = false;
                    } else {
                        stringBuilder.append(" ");
                    }
                    ++num;
                    stringBuilder.append(value);
                    if (num > 100000) return;
                }
            }

            Text urls = new Text(stringBuilder.toString());
            context.write(key, urls);
        }
    }
}