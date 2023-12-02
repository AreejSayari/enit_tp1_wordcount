package tn.enit.tp1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public static class TokenizerMapper extends Mapper<Object, Text, Text, NumPair> {
    private Text maritalStatus = new Text();
    private NumPair hours = new NumPair();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length >= 5) {
            /**
             * La méthode trim() est utilisée pour supprimer les espaces blancs autour de la valeur.
             */
            maritalStatus.set(fields[6].trim());
            hours.set(Integer.parseInt(fields[13].trim()), 1);
            context.write(maritalStatus, hours);
        }
    }
}