package org.example.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.example.entitys.HateSpeechTweet;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class HateSpeechBolt extends BaseBasicBolt {

    Map<String, String> conclusion = new HashMap<>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String tweet = tuple.getStringByField("text");

        String[] words = tweet.split("\\b");

        int wordsSize = words.length;
        int abusiveWordSize = 0;
        String isHateSpeech;

        for (String word : words) {
            if (HateSpeechBolt.AbusiveWords.get().contains(word)) {
                abusiveWordSize++;
            } else {
                isHateSpeech = "No";
                conclusion.put(tweet, isHateSpeech);
            }
        }

        if (abusiveWordSize > 0) {
            isHateSpeech = "Yes";
            conclusion.put(tweet, isHateSpeech);
        }

    }

    @Override
    public void cleanup() {
        System.out.println("\nKesimpulan Hate Speech sebuah tweet : ");
        for (Map.Entry<String, String> e : conclusion.entrySet()) {
            System.out.println(e.getKey() + " - " + e.getValue());
        }
        System.out.println();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    private static class AbusiveWords {
        private Set<String> abusiveWords;
        private static HateSpeechBolt.AbusiveWords _singleton;

        private AbusiveWords() {
            try {
                String fileName = "src/main/resources/abusive.csv";
                BufferedReader fileReader = new BufferedReader(new FileReader(fileName));
                String line;
                abusiveWords = new HashSet<>();

                while ((line = fileReader.readLine()) != null) {
                    abusiveWords.add(line);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        static HateSpeechBolt.AbusiveWords get() {
            if (_singleton == null) {
                synchronized (HateSpeechBolt.AbusiveWords.class) {
                    if (_singleton == null) {
                        _singleton = new HateSpeechBolt.AbusiveWords();
                    }
                }
            }

            return _singleton;
        }

        boolean contains(String key) {
            return get().abusiveWords.contains(key);
        }
    }
}
