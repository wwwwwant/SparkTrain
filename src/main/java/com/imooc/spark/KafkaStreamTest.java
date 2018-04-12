package com.imooc.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class KafkaStreamTest {

    private static final String zk = "192.168.3.249:2181";
    private static final String groupId = UUID.randomUUID().toString();
    private static final Map<String, String> kafkaParams = new HashMap<String, String>() {
        {
            put("group.id", groupId);
            put("zookeeper.connect", zk);
            put("socket.receive.buffer.bytes", "131072");
            put("fetch.message.max.bytes", "2097152");
            put("num.consumer.fetchers", "3");
            put("auto.offset.reset", "smallest");
        }
    };
    private static final Map<String,Integer> Topic = new HashMap<String,Integer>(){{
        put("wordCount",1);
    }};
    public static void main(String[] args) throws InterruptedException {

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaStream");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(5000));

//        JavaPairReceiverInputDStream<String,String> stream = KafkaUtils.createStream(
//                jssc
//                ,String.class
//                ,String.class
//                ,StringDecoder.class
//                ,StringDecoder.class
//                ,kafkaParams
//                ,Topic
//                ,StorageLevel.MEMORY_AND_DISK_SER());

        JavaPairInputDStream<String,String> stream = KafkaUtils.createStream(jssc,zk,groupId,Topic);

         JavaPairDStream<String,Integer> wordCounts= stream.map(v1 ->v1._2())
                .flatMap(line ->Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word,1))
                .reduceByKey((v1,v2)-> v1+v2);

         wordCounts.print();

         jssc.start();
         jssc.awaitTermination();
         jssc.close();



    }
}
