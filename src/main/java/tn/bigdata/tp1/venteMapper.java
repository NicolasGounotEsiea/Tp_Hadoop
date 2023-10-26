package tn.bigdata.tp1;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

public class venteMapper extends Mapper<Object, Text, Text, DoubleWritable>{
    private Text magasin = new Text();
    private DoubleWritable cout = new DoubleWritable();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] words = value.toString().split("\t");
        if (words.length >= 4) {
            try {
                double costValue = Double.parseDouble(words[4]);
                magasin.set(words[2]);
                cout.set(costValue);
                context.write(magasin, cout);
            } catch (NumberFormatException e) {
                System.err.println("Erreur de conversion du coût : " + e.getMessage());
            }
        }
    }
}
