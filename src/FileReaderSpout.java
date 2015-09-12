


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.Scanner;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class FileReaderSpout implements IRichSpout {
  private SpoutOutputCollector _collector;
  private TopologyContext context;
  private Scanner inputFile;
  public String fileName;
  public void open(Map conf, TopologyContext context,
                   SpoutOutputCollector collector) {

     /*
    ----------------------TODO-----------------------
    Task: initialize the file reader


    ------------------------------------------------- */
    this.context = context;
    this._collector = collector;
	    
	try {
		this.inputFile = new Scanner (new FileReader (fileName));

	} catch (FileNotFoundException e) {
		e.printStackTrace();
		System.exit(1);
	}

  }

  public void nextTuple() {

     /*
    ----------------------TODO-----------------------
    Task:
    1. read the next line and emit a tuple for it
    2. don't forget to sleep when the file is entirely read to prevent a busy-loop

    ------------------------------------------------- */
	    Utils.sleep(100);

			 while (inputFile.hasNextLine()){
			    	_collector.emit(new Values(inputFile.nextLine()));
			 }
			    
			 Utils.sleep(100);
	  
	   
	   
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {

    declarer.declare(new Fields("word"));

  }

  
  public void close() {
   /*
    ----------------------TODO-----------------------
    Task: close the file


    ------------------------------------------------- */
	 
	  inputFile.close();		
  }


  public void activate() {
  }

  public void deactivate() {
  }

  public void ack(Object msgId) {
  }

  public void fail(Object msgId) {
  }

  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
