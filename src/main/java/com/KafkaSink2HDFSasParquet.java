package com;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WordCountKafkaInStdOut2 {

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
        //properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-kafka");
        properties.setProperty("security.protocol","SASL_PLAINTEXT");
        properties.setProperty("sasl.kerberos.service.name","kafka");
        properties.setProperty("max.poll.records", String.valueOf(1000));
        System.setProperty("java.security.auth.login.config","/var/lib/hive/jaas.conf");


        // Source
        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<String>(inputTopic, new SimpleStringSchema(), properties);
        consumer.setStartFromLatest();
        DataStream<String> stream = env.addSource(consumer);


        // Transformations
        // 使用Flink算子对输入流的文本进行操作
        // 按空格切词、计数、分区、设置时间窗口、聚合
//        DataStream<Tuple2<String, Integer>> wordCount = stream
//                .flatMap((String line, Collector<Tuple2<String, Integer>> collector) -> {
//                    String[] tokens = line.split("\\s");
//                    // 输出结果 (word, 1)
//                    for (String token : tokens) {
//                        if (token.length() > 0) {
//                            collector.collect(new Tuple2<>(token, 1));
//                        }
//                    }
//                })
//                .returns(Types.TUPLE(Types.STRING, Types.INT))
//                .keyBy(0)
//                .timeWindow(Time.seconds(5))
//                .sum(1);


        // Sink
        //wordCount.print();
        //wordCount.writeAsText("/Users/danieltse/aa.txt");
        //写入hdfs
        final StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path(baseDir), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                            .withRolloverInterval(TimeUnit.MINUTES.toMillis(5))
                            .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
                            .withMaxPartSize(1024 * 1024 * 400)
                            .build())
                    .build();

        stream.addSink(sink);
        // execute
        env.execute("kafka streaming word count");
    }
}
