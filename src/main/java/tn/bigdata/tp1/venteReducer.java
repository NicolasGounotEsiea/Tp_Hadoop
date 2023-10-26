package tn.bigdata.tp1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class venteReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private DoubleWritable total = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double sum = 0.0;
        for (DoubleWritable value : values) {
            System.out.println("value: "+value.get());
            sum += value.get();
        }
        total.set(sum);
        System.out.println("--> Sum = "+sum);
        context.write(key, total);
    }
}