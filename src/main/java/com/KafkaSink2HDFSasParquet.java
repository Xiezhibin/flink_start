package com;

import com.utils.DayHourPathBucketer;
import com.utils.ParquetSinkWriter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaSink2HDFSasParquet {

    public static String brockers = "172.16.0.17:9092";
    
    public static String baseDir = "hdfs://hadoop3:8020/user/hive/warehouse";
    
    public static String topic = "test1";

   
    static  String schema = "{\"name\" :\"UserGroup\"" +
            ",\"type\" :\"record\"" +
            ",\"fields\" :" +
            "     [ {\"name\" :\"data\",\"type\" :\"string\" }] " +
            "} ";

    public static GenericData.Record transformData(String msg) {
        GenericData.Record record = new GenericData.Record(Schema.parse(schema));
        record.put("data", msg);
        return record;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(20 * 60 * 1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        Properties props = new Properties();
	props.setProperty("security.protocol","SASL_PLAINTEXT");
	props.setProperty("sasl.kerberos.service.name","kafka");
        System.out.println("Create productor");
	System.setProperty("java.security.auth.login.config","/var/lib/hive/jaas.conf");
        props.setProperty("bootstrap.servers", brockers);
        props.setProperty("group.id", "flink.cluster.1");


        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(topic,
                new SimpleStringSchema(), props);
        consumer.setStartFromLatest();
        DataStreamSource<String> stream = env.addSource(consumer);

        SingleOutputStreamOperator<GenericData.Record> output = stream.map(KafkaSink2HDFSasParquet::transformData);


        BucketingSink<GenericData.Record> sink = new BucketingSink<GenericData.Record>(baseDir);
        sink.setBucketer(new DayHourPathBucketer());
        sink.setBatchSize(1024 * 1024 * 400); //400M
        ParquetSinkWriter writer = new ParquetSinkWriter<GenericData.Record>(schema);
        sink.setWriter(writer);


        output.addSink(sink);
        env.execute("KafkaSink2HDFSasParquet");
    }
}
