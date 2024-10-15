import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Random;

public class TimestampProcessor {

    public static void main(String[] args) throws Exception {
        // 设置执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建一个源，生成模拟的时间戳数据
        DataStream<Long> timestampStream = env.addSource(new TimestampSource());

        // 处理时间戳
        DataStream<Tuple2<Long, Integer>> processedStream = timestampStream
            .map(new MapFunction<Long, Tuple2<Long, Integer>>() {
                @Override
                public Tuple2<Long, Integer> map(Long timestamp) throws Exception {
                    return new Tuple2<>(timestamp, 1);
                }
            })
            .keyBy(value -> value.f0 / 1000) // 按秒分组
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            .sum(1);

        // 打印结果
        processedStream.print();

        // 执行作业
        env.execute("Timestamp Processor");
    }

    // 模拟时间戳源
    public static class TimestampSource implements SourceFunction<Long> {
        private volatile boolean isRunning = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            while (isRunning) {
                long timestamp = System.currentTimeMillis();
                // 模拟可能的时间戳遗漏
                if (random.nextDouble() > 0.1) { // 90% 的概率发送时间戳
                    ctx.collect(timestamp);
                }
                Thread.sleep(100); // 每100毫秒生成一个时间戳
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}