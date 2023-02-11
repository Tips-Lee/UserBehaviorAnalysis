package com.tips.market_analysis;

import com.tips.market_analysis.beans.AdClickEvent;
import com.tips.market_analysis.beans.AdClickWarning;
import com.tips.market_analysis.beans.AdCountViewByProv;
import com.tips.market_analysis.beans.BlackListUserWarning;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.sql.Timestamp;

public class AdStatsByProv {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 从文件中读取数据
        String path = "/AdClickLog.csv";
        URL resource = AdStatsByProv.class.getResource(path);
        System.out.println(resource);
        DataStream<String> inputStream = env.readTextFile(resource.getPath());

        DataStream<AdClickEvent> dataStream = inputStream.map(line->{
            String[] fields = line.split(",");
            return new AdClickEvent(Long.valueOf(fields[0]), Long.valueOf(fields[1]), fields[2], fields[3], Long.valueOf(fields[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickEvent>() {
            @Override
            public long extractAscendingTimestamp(AdClickEvent element) {
                return element.getTimestamp()*1000L;
            }
        });

        // 过滤异常
        SingleOutputStreamOperator<AdClickEvent> filterStream = dataStream.keyBy("userId", "adId")
                .process(new AdKeyedProcessFunction(100L));

        SingleOutputStreamOperator<AdCountViewByProv> outputStream = dataStream.keyBy(AdClickEvent::getProvince)
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new AdAgg(), new AdWin());

        outputStream.print("output");
        filterStream.getSideOutput(new OutputTag<>("black-list") {}).print("black-list");
        env.execute("ad count by province");
    }

    public static class AdKeyedProcessFunction extends KeyedProcessFunction<Tuple, AdClickEvent, AdClickEvent>{
        private final Long threshold;
        ValueState<Long> countState;
        ValueState<Boolean> isSentState;

        public AdKeyedProcessFunction(Long threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<>("count-state", Long.class, 0L));
            isSentState = getRuntimeContext().getState(new ValueStateDescriptor<>("is-sent-state", Boolean.class, false));
        }

        @Override
        public void processElement(AdClickEvent value, Context ctx, Collector<AdClickEvent> out) throws Exception {
            Long count = countState.value();

            if(count==0){
                long timer = (ctx.timerService().currentProcessingTime() / (24*60*60*1000) + 1) * (24*60*60*1000) + (8*60*60*1000);
                ctx.timerService().registerProcessingTimeTimer(timer);
            }

            if (count >= threshold){
                if (!isSentState.value()){
                    isSentState.update(true);
                    OutputTag<BlackListUserWarning> outputTag = new OutputTag<>("black-list") {};
                    ctx.output(outputTag, new BlackListUserWarning(value.getUserId(), value.getAdId(), "click over " + count + " times"));
                }
                return;
            }
            countState.update(count + 1L);
            out.collect(value);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) {
            countState.clear();
            isSentState.clear();
        }
    }

    public static class AdAgg implements AggregateFunction<AdClickEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent value, Long accumulator) {
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

    public static class AdWin implements WindowFunction<Long, AdCountViewByProv, String, TimeWindow> {
        @Override
        public void apply(String province, TimeWindow window, Iterable<Long> input, Collector<AdCountViewByProv> out) {
            String windowEnd =new Timestamp(window.getEnd()).toString();
            out.collect(new AdCountViewByProv(province, windowEnd, input.iterator().next()));
        }
    }
}
