package com.hiya.da.strom.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.hiya.da.strom.bolt.HiyaWordArrCountBolt;
import com.hiya.da.strom.bolt.HiyaWordArrSplitBolt;
import com.hiya.da.strom.spout.HiyaKafkaSpout;

public class HiyaKafkaTopology
{
	public static void main(String[] args)
	{
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("testGroup", new HiyaKafkaSpout("test"));
		builder.setBolt("file-blots", new HiyaWordArrSplitBolt()).shuffleGrouping("testGroup");
		builder.setBolt("words-counter", new HiyaWordArrCountBolt(), 2).fieldsGrouping("file-blots", new Fields("words"));
		Config config = new Config();
		config.setDebug(true);	
		if (args != null && args.length > 0)
		{
			config.put(Config.NIMBUS_HOST, args[0]);
			config.setNumWorkers(3);
			try
			{
				StormSubmitter.submitTopologyWithProgressBar(HiyaKafkaTopology.class.getSimpleName(), config,
						builder.createTopology());
			} catch (Exception e)
			{
				e.printStackTrace();
			}
		} else
		{
			LocalCluster local = new LocalCluster();
			local.submitTopology("counter", config, builder.createTopology());
			try
			{
				Thread.sleep(60000);
			} catch (InterruptedException e)
			{
				e.printStackTrace();
			}
			local.shutdown();
		}
	}
}
