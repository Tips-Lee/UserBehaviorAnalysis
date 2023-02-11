package com.tips.market_analysis;

import com.tips.market_analysis.beans.ChannelPromotionCount;
import com.tips.market_analysis.beans.MarketingUserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class AppMarketingByChannel {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1. 读取数据
        DataStream<MarketingUserBehavior> inputStream = env.addSource(new SimulatedMarketingUserBehaviorSource())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<MarketingUserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(MarketingUserBehavior element) {
                        return element.getTimestamp();
                    }
                });

        SingleOutputStreamOperator<ChannelPromotionCount> outputStream = inputStream.filter(x -> !"UNINSTALL".equals(x.getBehavior()))
                .keyBy("channel", "behavior")
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new AggregateFunctionUDC(), new ProcessWindowFunctionUDC());

        outputStream.print();
        env.execute("app marketing by channel");
    }

    public static class AggregateFunctionUDC implements AggregateFunction<MarketingUserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(MarketingUserBehavior value, Long accumulator) {
            return accumulator+1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a+b;
        }
    }


    public static class ProcessWindowFunctionUDC extends ProcessWindowFunction<Long, ChannelPromotionCount, Tuple, TimeWindow> {
        @Override
        public void process(Tuple key, Context context, Iterable<Long> elements, Collector<ChannelPromotionCount> out) {
            String channel = key.getField(0);
            String behavior = key.getField(1);
            String windowEnd = new Timestamp(context.window().getEnd()).toString();
            Long count = elements.iterator().next();
            out.collect(new ChannelPromotionCount(channel, behavior, windowEnd, count));
        }
    }


    public static class SimulatedMarketingUserBehaviorSource implements SourceFunction<MarketingUserBehavior>{
        Boolean running=true;
        List<String> behaviorList = Arrays.asList("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL");
        List<String> channelList = Arrays.asList("app store", "wechat", "weibo");
        Random r = new Random();

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (running){
                // 随机生成所有字段
                Long id = r.nextLong();
                String behavior = behaviorList.get(r.nextInt(behaviorList.size()));
                String channel = channelList.get(r.nextInt(channelList.size()));
                Long timestamp = System.currentTimeMillis();

                // 发出数据
                ctx.collect(new MarketingUserBehavior(id, behavior, channel, timestamp));
                Thread.sleep(100L);
            }
        }

        @Override
        public void cancel() {
            running=false;
        }
    }
}
