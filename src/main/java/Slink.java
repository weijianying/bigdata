import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;
/**
 * 测试flink读取kafka写入hdfs
 * */
public class Slink {
    public static void main(String[] args) throws Exception {
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("10.20.80.201:9092")
                .setTopics("flink_cdc")
                .setGroupId("hdfs")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(3000);

        DataStreamSource<String> kafka_source = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");


        String path = "hdfs://10.20.80.201:9000/hivedata/dd";
        final StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path(path), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(30))//多长时间运行一个文件
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(10))//多长时间没有写入就生成一个文件
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();
//        指定登录hdfs的用户为root
        System.setProperty("HADOOP_USER_NAME","root");

        kafka_source.print();
        kafka_source.addSink(sink);
        env.execute("hdfs");

    }
}
