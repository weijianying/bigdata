import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

public class SinkHdfs {
    public static void main(String[] args) throws Exception {
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("10.20.80.201:9092")
                .setTopics("flink_cdc")
                .setGroupId("hdfs")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://10.20.80.201:9000/flink-checkpoint/checkpoint");
        //设置两个Checkpoint 之间最少等待时间,如设置Checkpoint之间最少是要等 500ms(为了避免每隔1000ms做一次Checkpoint的时候,前一次太慢和后一次重叠到一起去了  --//默认是0
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //设置Checkpoint的时间间隔为1000ms做一次Checkpoint/其实就是每隔1000ms发一次Barrier!
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置checkpoint的超时时间,如果 Checkpoint在 60s内尚未完成说明该次Checkpoint失败,则丢弃。
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //设置同一时间有多少个checkpoint可以同时执行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        // 设置重启策略
        // 一个时间段内的最大失败次数
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3,
                // 衡量失败次数的时间段
                Time.of(5, TimeUnit.MINUTES),
                // 间隔
                Time.of(10, TimeUnit.SECONDS)
        ));
        DataStreamSource<String> kafka_source = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");


        String path = "hdfs://10.20.80.201:9000/hivedata/device_flowdata";
        final StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path(path), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(30))//多长时间运行一个文件
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(10))//多长时间没有写入就生成一个文件
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();
        System.setProperty("HADOOP_USER_NAME","root");

        kafka_source.print();
        kafka_source.addSink(sink);

        env.execute();


    }
}
