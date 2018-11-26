package com.hiya.da.strom.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.hiya.da.strom.bolt.HiyaWordArrCountBolt;
import com.hiya.da.strom.bolt.HiyaWordArrSplitBolt;
import com.hiya.da.strom.spout.HiyaWordArrSpout;

public class HiyaWordArrTopology
{
	private static final String test_spout = "Hiya_WordArr_Spout";
	private static final String test_bolt = "Hiya_WordArr_Split_Bolt";
	private static final String test2_bolt = "Hiya_WordArr_Count_Bolt";

	public static void main(String[] args)
	{
		// 定义一个拓扑
		TopologyBuilder builder = new TopologyBuilder();
		// 设置一个Executeor(线程)，默认一个
		builder.setSpout(test_spout, new HiyaWordArrSpout(), 1);
		// shuffleGrouping:表示是随机分组
		// 设置一个Executeor(线程)，和一个task
		builder.setBolt(test_bolt, new HiyaWordArrSplitBolt(), 1).setNumTasks(1).shuffleGrouping(test_spout);
		// fieldsGrouping:表示是按字段分组
		// 设置一个Executeor(线程)，和一个task
		builder.setBolt(test2_bolt, new HiyaWordArrCountBolt(), 1).setNumTasks(1).fieldsGrouping(test_bolt,new Fields("count"));
		Config conf = new Config();
		conf.put("test", "test");
		try
		{
			// 运行拓扑
			if (args != null && args.length > 0)
			{ // 有参数时，表示向集群提交作业，并把第一个参数当做topology名称
				System.out.println("运行远程模式");
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} else
			{// 没有参数时，本地提交
				// 启动本地模式
				System.out.println("运行本地模式");
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("Word-counts", conf, builder.createTopology());
				Thread.sleep(20000);
				// //关闭本地集群
				cluster.shutdown();
			}
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}