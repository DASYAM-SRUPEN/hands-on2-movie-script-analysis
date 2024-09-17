package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DialogueLengthReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalWords = 0;

        // Sum the word counts for each character
        for (IntWritable value : values) {
            totalWords += value.get();
        }

        // Write the character and the total word count
        context.write(key, new IntWritable(totalWords));
    }
}
