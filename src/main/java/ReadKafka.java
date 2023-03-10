import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;

public class ReadKafka {
    public static void main(String[] args){

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("10.20.80.201:9092")
                .setTopics("kafka_cdc")
                .setGroupId("tt")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("10.20.80.201:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("flink_cdc")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        Configuration configuration = new Configuration();
        configuration.setString(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, "8848a383bbda8a237e333a5ba2c205e3");
/**
 * ????????????linux????????????????????????,?????????File.separator??????????????????????????????linux??????
 * ??????????????????chk?????????????????????????????????????????????????????????
 * ???????????????????????????????????????JobID,??????????????????chk????????????????????????chk??????????????????chk???????????????,??????????????????chk???????????????????????????????????????????????????????????????
 * ??????????????????,???????????????????????????????????????????????????????????????????????????
 * ????????????????????????configuration?????????flink???????????????????????????
 * ???????????????????????????????????????,??????????????????,??????????????????????????????????????????,
 * ????????????????????????configuration.setString("execution.savepoint.path","file://"+lastModified.toString());
 * ???flink???????????????
 * */
        File dir = new File("file:///tmp/flink-checkpoints/8848a383bbda8a237e333a5ba2c205e3");
        if (dir.exists()) {
            File[] files = dir.listFiles();
            assert files != null;
            for (File file : files) {
                System.out.println(file);
            }
            File lastModified = Arrays.stream(files)
                    .filter(File::isDirectory)
                    .max(Comparator.comparing(File::lastModified))
                    .orElse(null);
            assert lastModified != null;
            System.out.println(lastModified.toString());

            configuration.setString("execution.savepoint.path", lastModified.toString());
            System.out.println("file://" + lastModified.toString());
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.enableCheckpointing(3000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-checkpoints");

        DataStreamSource<String> kafka_source = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        SingleOutputStreamOperator<String> filter = kafka_source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String data) throws Exception {
                JSONObject dataJson = JSON.parseObject(data);
                return dataJson.get("op").equals("r");
            }
        });
        filter.print();
        SingleOutputStreamOperator<String> map = filter.map(new MapFunction<String, String>() {
            @Override
            public String map(String data) {
                JSONObject dataJson = JSON.parseObject(data);
                dataJson.remove("source");
                String s = JSONObject.toJSONString(dataJson, SerializerFeature.WriteMapNullValue);
                return s;
            }
        });

//
        KeyedStream<String, String> key = map.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String data) throws Exception {
                JSONObject dataJson = JSON.parseObject(data);
                JSONObject after = dataJson.getJSONObject("after");
                String sex = after.getString("sex");
                return sex;
            }
        });
        WindowedStream<String,
                String, TimeWindow> window = key.window(TumblingEventTimeWindows.of(Time.seconds(3)));

        key.print();
//        map.sinkTo(sink);

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
