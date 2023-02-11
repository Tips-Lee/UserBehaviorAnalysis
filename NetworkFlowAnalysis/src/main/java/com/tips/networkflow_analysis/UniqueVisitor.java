package com.tips.networkflow_analysis;

import com.tips.networkflow_analysis.beans.PageViewCount;
import com.tips.networkflow_analysis.beans.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
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
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class UniqueVisitor {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //  文件数据源
        String inputPath ="/UserBehavior.csv";
        URL resource = UniqueVisitor.class.getResource(inputPath);
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

        SingleOutputStreamOperator<PageViewCount> outputStream = dataStream.filter(x -> "pv".equals(x.getBehavior()))
                .timeWindowAll(Time.hours(1))
                .apply(new AllWindowFunctionImpl());

        outputStream.print();
        env.execute("uv analysis");
    }

    public static class AllWindowFunctionImpl implements AllWindowFunction<UserBehavior, PageViewCount, TimeWindow>{

        @Override
        public void apply(TimeWindow window, Iterable<UserBehavior> values, Collector<PageViewCount> out) throws Exception {
            Set<Long> uidSet = new HashSet<>();
            for (UserBehavior value: values) {
                uidSet.add(value.getUserId());
            }
            out.collect(new PageViewCount("uv", window.getEnd(), (long) uidSet.size()));

        }
    }

}
