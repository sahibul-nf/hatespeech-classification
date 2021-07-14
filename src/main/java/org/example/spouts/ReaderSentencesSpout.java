package org.example.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.example.entitys.HateSpeechTweet;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ReaderSentencesSpout extends BaseRichSpout {

    String line;
    private SpoutOutputCollector collector;
    private List<HateSpeechTweet> data = new ArrayList<HateSpeechTweet>();
    boolean isCompleted;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;

        try {
            String fileName = "src/main/resources/data.csv";
            BufferedReader fileReader = new BufferedReader(new FileReader(fileName));

            while ((line = fileReader.readLine()) != null) {
                String[] tweets = line.split(",");

                HateSpeechTweet hateSpeechTweet = new HateSpeechTweet();
                hateSpeechTweet.setTweet(tweets[0]);

                    data.add(hateSpeechTweet);

            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        int i = 0;
        if (!isCompleted) {
            for (HateSpeechTweet tweet : data) {

                if (tweet.getTweet().length() < 1000) {
                    i++;
                    System.out.println("Emit yang ke " +i);
                    collector.emit(new Values(tweet.getTweet()));
                }
            }
            isCompleted = true;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("text"));
    }
}
