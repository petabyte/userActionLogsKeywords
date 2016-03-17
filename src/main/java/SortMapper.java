import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by gsanchez on 2/25/16.
 */
public class SortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    Text keyWordText = new Text();
    IntWritable countWritable = new IntWritable();
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String lineRecord = value.toString();
            String [] lineSplit = lineRecord.split("\\t+");
            String firstElement = lineSplit[0];
            String secondElement = lineSplit[1];
            keyWordText.set(firstElement);
            countWritable.set(Integer.parseInt(secondElement));
            context.write(countWritable, keyWordText);
    }
}
