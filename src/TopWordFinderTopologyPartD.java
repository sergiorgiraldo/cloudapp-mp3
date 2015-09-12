


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * This topology reads a file and counts the words in that file, then finds the top N words.
 */
public class TopWordFinderTopologyPartD {

  private static final int N = 10;

  public static void main(String[] args) throws Exception {


    TopologyBuilder builder = new TopologyBuilder();

    Config config = new Config();
    config.setDebug(true);


    /*
    ----------------------TODO-----------------------
    Task: wire up the topology

    NOTE:make sure when connecting components together, using the functions setBolt(name,…) and setSpout(name,…),
    you use the following names for each component:
    
    FileReaderSpout -> "spout"
    SplitSentenceBolt -> "split"
    WordCountBolt -> "count"
	NormalizerBolt -> "normalize"
    TopNFinderBolt -> "top-n"


    ------------------------------------------------- */
    FileReaderSpout fs = new FileReaderSpout();
    fs.fileName = args[0];
    
    builder.setSpout("spout", fs, 5);
    builder.setBolt("split", new SplitSentenceBolt(), 8).shuffleGrouping("spout");
    builder.setBolt("normalize", new NormalizerBolt(),12).fieldsGrouping("split", new Fields("word"));
    builder.setBolt("count", new WordCountBolt(), 16).fieldsGrouping("normalize", new Fields("norm&filtered-word"));
    builder.setBolt("top-n", new TopNFinderBolt(10), 16).fieldsGrouping("count", new Fields("count","word"));
    

    config.setMaxTaskParallelism(3);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("word-count", config, builder.createTopology());

    //wait till the file is read completely
    Thread.sleep(2 * 60 * 1000);

    cluster.shutdown();
  }
}
