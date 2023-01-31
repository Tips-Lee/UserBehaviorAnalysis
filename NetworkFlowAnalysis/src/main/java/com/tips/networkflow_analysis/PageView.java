package com.tips.networkflow_analysis;

import com.tips.networkflow_analysis.beans.PageViewCount;
import com.tips.networkflow_analysis.beans.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import java.util.Random;

public class PageView {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //  文件数据源
        String inputPath ="/UserBehavior.csv";
        URL resource = PageView.class.getResource(inputPath);
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

        SingleOutputStreamOperator<PageViewCount> agg = dataStream.filter(x -> "pv".equals(x.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(UserBehavior value) {
                        Random r = new Random();
                        return Tuple2.of(r.nextInt(10), 1);
                    }
                })
                .keyBy(x -> x.f0)
                .timeWindow(Time.hours(1))
                .aggregate(new PvAggregateFunction(), new PvWindowFunction());

        DataStream<PageViewCount> outputStream = agg.keyBy(PageViewCount::getWindowEnd)
                .process(new PvKeyedProcessFunction());

        outputStream.print();
        env.execute("hot items analysis");
    }

    public static class PvKeyedProcessFunction extends KeyedProcessFunction<Long, PageViewCount, PageViewCount>{
        ValueState<Long> valueState;

        @Override
        public void open(Configuration parameters) {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<>("value-state", Long.class, 0L));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<PageViewCount> out) throws IOException {
            Long value = valueState.value();
            out.collect(new PageViewCount("pv", ctx.getCurrentKey(), value));
            valueState.clear();
        }

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<PageViewCount> out) throws IOException {
            valueState.update(value.getCnt() + valueState.value());
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+1L);
        }
    }

    public static class PvWindowFunction implements WindowFunction<Long, PageViewCount, Integer, TimeWindow>{
        @Override
        public void apply(Integer key, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
            out.collect(new PageViewCount(key.toString(), window.getEnd(), input.iterator().next()));
        }

    }

    public static class PvAggregateFunction implements AggregateFunction<Tuple2<Integer, Integer>, Long, Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<Integer, Integer> value, Long accumulator) {
            return accumulator+1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }
}
