package com.tips.networkflow_analysis;

import akka.pattern.Patterns;
import com.tips.networkflow_analysis.beans.ApacheLogEvent;
import com.tips.networkflow_analysis.beans.PageViewCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class HotPages {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1. 读取数据
        URL url = HotPages.class.getResource("/apache.log");
        DataStream<String> inputStream = env.readTextFile(url.getPath());

        // 2. 数据及其Watermarks预处理
        DataStream<ApacheLogEvent> dataStream = inputStream.map(line->{
            String[] fields = line.split(" ");
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            Long ts = simpleDateFormat.parse(fields[3]).getTime();
            return new ApacheLogEvent(fields[0], fields[1], ts, fields[5], fields[6]);
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(ApacheLogEvent element) {
                return element.getTimestamp();
            }
        });

        // 3. 分组开窗聚合

        OutputTag<ApacheLogEvent> outputTag = new OutputTag<>("late"){};

        SingleOutputStreamOperator<PageViewCount> agg = dataStream.filter(x -> "GET".equals(x.getMethod()))
                .filter(x->{
                    String regex = "^((?!\\.(css|js|ico|png)$).)*$";
                    return Pattern.matches(regex, x.getUrl());
                })
                .keyBy(ApacheLogEvent::getUrl)
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .aggregate(new AggregateFunctionImpl(), new WindowFunctionImpl());

        agg.print("agg");
        agg.getSideOutput(outputTag).print("late");

        // 4. 同一窗口按照cnt逆序输出
        DataStream<String> outputStream = agg.keyBy(PageViewCount::getWindowEnd)
                .process(new KeyedProcessFunctionImpl(5));

        outputStream.print();
        env.execute();
    }

    public static class KeyedProcessFunctionImpl extends KeyedProcessFunction<Long, PageViewCount, String>{
        private final Integer topSize;

        MapState<String, PageViewCount> pageViewCountMapState;

        public KeyedProcessFunctionImpl(Integer topSize) {
            this.topSize = topSize;
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            if (timestamp==ctx.getCurrentKey()+60*1000L) {
                pageViewCountMapState.clear();
                return;
            }

            List<Map.Entry<String, PageViewCount>> entries = Lists.newArrayList(pageViewCountMapState.entries().iterator());
            entries.sort((o1, o2) -> o2.getValue().getCnt().compareTo(o1.getValue().getCnt()));
            StringBuilder sb = new StringBuilder();
            sb.append("=============================\n")
                    .append("窗口结束时间:")
                    .append(new Timestamp(ctx.getCurrentKey()))
                    .append("\n");
            for (int i = 0; i < Math.min(topSize, entries.size()); i++) {
                PageViewCount pageViewCount = entries.get(i).getValue();
                sb.append("No. ").append(i+1)
                        .append("\turl: ").append(pageViewCount.getUrl())
                        .append("\tpageCount: ").append(pageViewCount.getCnt())
                        .append("\twindowEnd: ").append(new Timestamp(pageViewCount.getWindowEnd()))
                        .append("\n");
            }
            sb.append("=============================\n");

            Thread.sleep(100L);
            out.collect(sb.toString());
        }

        @Override
        public void open(Configuration parameters) {
            pageViewCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("map-state", String.class, PageViewCount.class));
        }

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<String> out) throws Exception {
            pageViewCountMapState.put(value.getUrl(), value);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+1L);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+60*1000L);
        }
    }

    public static class WindowFunctionImpl implements WindowFunction<Long, PageViewCount, String, TimeWindow> {
        @Override
        public void apply(String key, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) {
            PageViewCount record = new PageViewCount(key, window.getEnd(), input.iterator().next());
            out.collect(record);
        }
    }

    public static class AggregateFunctionImpl implements AggregateFunction<ApacheLogEvent, Long, Long>{
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent value, Long accumulator) {
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
