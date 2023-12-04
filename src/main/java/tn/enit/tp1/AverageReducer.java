package tn.enit.tp1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AverageReducer extends Reducer<Text, NumPair, Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<NumPair> values, Context context) throws IOException, InterruptedException {
        int sumHours = 0;
        int count = 0;

        for (NumPair value : values) {
            sumHours += value.getFirst();
            count += value.getSecond();
        }

        if (count > 0) {
            result.set((double) sumHours / count);
            context.write(key, result);
        }
    }
}