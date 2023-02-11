package com.tips.networkflow_analysis;

import com.tips.networkflow_analysis.beans.PageViewCount;
import com.tips.networkflow_analysis.beans.UserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.net.URL;
import java.util.HashSet;
import java.util.Set;

public class UvWithBloomFilter {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //  文件数据源
        String inputPath ="/UserBehavior.csv";
        URL resource = UvWithBloomFilter.class.getResource(inputPath);
        DataStream<String> inputStream = env.readTextFile(resource.getPath());

        DataStream<UserBehavior> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(Long.valueOf(fields[0]), Long.valueOf(fields[1]), Integer.valueOf(fields[2]), fields[3], Long.valueOf(fields[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000L;
            }
        });

        DataStream<PageViewCount> outputStream = dataStream.filter(x -> "pv".equals(x.getBehavior()))
                .timeWindowAll(Time.hours(1))
                .trigger(new MyWindowTrigger())
                .process(new ProcessAllWindowFunctionImpl());

        outputStream.print();
        env.execute("uv analysis with bloom filter");
    }

    public static class MyWindowTrigger extends Trigger<UserBehavior, TimeWindow> {

        @Override
        public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) {
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) {

        }
    }

    public static class ProcessAllWindowFunctionImpl extends ProcessAllWindowFunction<UserBehavior, PageViewCount, TimeWindow>{
        Jedis jedis;
        MyBloomFilter myBloomFilter;

        @Override
        public void open(Configuration parameters) {
            jedis = new Jedis("localhost", 6379);
            // jedis.auth("xxxx");
            myBloomFilter = new MyBloomFilter(1<<29);
        }

        @Override
        public void process(Context context, Iterable<UserBehavior> elements, Collector<PageViewCount> out) {
            long windowEnd = context.window().getEnd();
            Long userId = elements.iterator().next().getUserId();

            // bitmap 按照windowEnd 存储
            String bitMapKey = Long.toString(windowEnd);

            // count 存在 hash 表里
            String countHashName = "uv_count";
            String countFieldName = Long.toString(windowEnd);

            Long offset = myBloomFilter.hash(userId.toString(), 21);

            Boolean isExist = jedis.getbit(bitMapKey, offset);
            if (!isExist){
                jedis.setbit(bitMapKey,offset, true);
                jedis.expire(bitMapKey, 60);
                long count = 0L;
                String countString = jedis.hget(countHashName, countFieldName);
                if (countString != null && !"".equals(countString)) count = Long.parseLong(countString);
                jedis.hset(countHashName, countFieldName, String.valueOf(count + 1));
                out.collect(new PageViewCount("uv", windowEnd, count+1));
            }

        }

        @Override
        public void close() {
            jedis.close();
        }
    }

    public static class MyBloomFilter{
        private Integer cap;

        public MyBloomFilter(Integer cap) {
            this.cap = cap;
        }

        public Long hash(String value, Integer seed){
            long hashCode = 0L;
            for (int i = 0; i < value.length(); i++) {
                hashCode = hashCode * seed + value.charAt(i);
            }

            return hashCode & (cap-1);
        }
    }

}
