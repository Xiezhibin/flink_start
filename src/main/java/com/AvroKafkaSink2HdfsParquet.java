package com;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.Schema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import java.util.Properties;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;


public class AvroKafkaSink2HdfsParquet {

    public static final String USER_SCHEMA = "{\n" +
            "    \"type\":\"record\",\n" +
            "    \"name\":\"Customer\",\n" +
            "    \"fields\":[" +
            "        {\"name\":\"id\",\"type\":\"int\"}," +
            "        {\"name\":\"name\",\"type\":\"string\"}," +
            "        {\"name\":\"email\",\"type\":\"string\"}" +
            "    ]}";

    public static String brockers = "172.16.0.17:9092";
    public static String inputTopic = "test1";
    public static String baseDir = "hdfs://hadoop3:8020/user/hive/warehouse";
    public static void main(String[] args) throws Exception {

        //创建flink执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //配置kafka参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", brockers);
        properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
//        properties.setProperty("max.poll.records", String.valueOf(1000));
        properties.setProperty("group.id", "flink-kafka");
        properties.setProperty("security.protocol","SASL_PLAINTEXT");
        properties.setProperty("sasl.kerberos.service.name","kafka");
        properties.setProperty("max.poll.records", String.valueOf(1000));
        System.setProperty("java.security.auth.login.config","/var/lib/hive/jaas.conf");

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(USER_SCHEMA);

        GenericRecord genericRecord = new GenericData.Record(schema);
        // Source
        FlinkKafkaConsumer<GenericRecord> consumer =
                new FlinkKafkaConsumer<GenericRecord>(inputTopic, (DeserializationSchema<GenericRecord>) genericRecord, properties);
        consumer.setStartFromLatest();
        DataStream<GenericRecord> stream = env.addSource(consumer);

        final StreamingFileSink<GenericRecord> sink = StreamingFileSink
                .forBulkFormat(new Path(baseDir), ParquetAvroWriters.forGenericRecord(schema))
                .build();

        stream.addSink(sink);
        // execute
        env.execute("kafka streaming word count");
    }
}

