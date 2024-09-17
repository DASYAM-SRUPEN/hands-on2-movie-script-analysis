package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class CharacterWordMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text characterWord = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        int splitIndex = line.indexOf(':');
        
        if (splitIndex > 0) {
            String character = line.substring(0, splitIndex).trim();  // Extract character name
            String dialogue = line.substring(splitIndex + 1).trim();  // Extract dialogue
            
            // Tokenize the dialogue to get individual words
            StringTokenizer tokenizer = new StringTokenizer(dialogue);
            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase(); // Remove punctuation, make lowercase
                if (!word.isEmpty()) {
                    characterWord.set(character + "-" + word);  // Set the key as "Character-Word"
                    context.write(characterWord, one);  // Emit the key-value pair (Character-Word, 1)
                }
            }
        }
    }
    }
