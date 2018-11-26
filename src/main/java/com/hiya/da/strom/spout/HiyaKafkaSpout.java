package com.hiya.da.strom.spout;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class HiyaKafkaSpout implements IRichSpout
{
	private static final long serialVersionUID = -7107773519958260350L;
	SpoutOutputCollector collector;
	private ConsumerConnector consumer;
	private String topic;

	private static ConsumerConfig createConsumerConfig()
	{
		Properties props = new Properties();
		props.put("zookeeper.connect", "");
		props.put("group.id", "");
		props.put("zookeeper.session.timeout.ms", "40000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		return new ConsumerConfig(props);
	}

	public HiyaKafkaSpout(String topic)
	{
		this.topic = topic;
	}

	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector)
	{
		this.collector = collector;
	}

	public void close()
	{

	}

	public void activate()
	{
		this.consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());
		Map<String, Integer> topickMap = new HashMap<String, Integer>();
		topickMap.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> streamMap = consumer.createMessageStreams(topickMap);
		KafkaStream<byte[], byte[]> stream = streamMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while (it.hasNext())
		{
			String value = new String(it.next().message());
			System.out.println("(consumer)==>" + value);
			collector.emit(new Values(value), value);
		}
	}

	public void deactivate()
	{

	}

	public void nextTuple()
	{

	}

	public void ack(Object msgId)
	{

	}

	public void fail(Object msgId)
	{

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("KafkaSpout"));
	}

	public Map<String, Object> getComponentConfiguration()
	{
		return null;
	}
}
