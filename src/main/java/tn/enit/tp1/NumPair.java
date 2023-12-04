package tn.enit.tp1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.io.InputStream;

/*
le nombre total d'heures de travail et le nombre d'occurrences associées à chaque état civil (marital status).
 */
public class NumPair implements Writable {
    PrintStream out = System.out;
    InputStream in = System.in;


    private int first;
    private int second;

    public NumPair() {}

    public NumPair(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public int getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    public void set(int first, int second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        out.write(first);
        out.write(second);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first = in.read();
        second = in.read();
    }


}
