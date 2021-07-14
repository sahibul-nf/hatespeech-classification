package org.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import org.example.bolts.HateSpeechBolt;
import org.example.spouts.ReaderSentencesSpout;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new ReaderSentencesSpout());
        builder.setBolt("bolt", new HateSpeechBolt(), 1).shuffleGrouping("spout");

        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("hatespeech", config, builder.createTopology());

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        cluster.killTopology("hatespeech");
        cluster.shutdown();
    }
}
