package com.tips.hotitems_analysis;

import com.tips.hotitems_analysis.beans.ItemViewCount;
import com.tips.hotitems_analysis.beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Properties;


public class HotItems {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //  文件数据源
        /*String inputPath ="./HotItemsAnalysis/src/main/resources/UserBehavior.csv";
        DataStream<String> inputStream = env.readTextFile(inputPath);*/

        // Kafka 数据源
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "zk1:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer<String>("hot-items", new SimpleStringSchema(), properties));

        DataStream<UserBehavior> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(Long.valueOf(fields[0]), Long.valueOf(fields[1]), Integer.valueOf(fields[2]), fields[3], Long.valueOf(fields[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000L;
            }
        });


        DataStream<ItemViewCount> windowAggStream = dataStream.filter(x->"pv".equals(x.getBehavior()))
                .keyBy("itemId")
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new ItemCountAggFunc(), new ItemCountWindowFunc());

        DataStream<String> outputStream = windowAggStream.keyBy("windowEnd")
                .process(new TopNHotItems(5));

        outputStream.print();
        env.execute("hot items analysis");
    }

    public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {
        private int n;
        ListState<ItemViewCount> itemViewCountListState;
        public TopNHotItems(int n) {
            this.n = n;
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());
            itemViewCounts.sort((o1, o2) -> o2.getCount().intValue() - o1.getCount().intValue());
            StringBuilder sb = new StringBuilder();
            sb.append("=============================\n")
                    .append("窗口结束时间:")
                    .append(new Timestamp(ctx.getCurrentKey().getField(0)))
                    .append("\n");
            for (int i = 0; i < Math.min(n, itemViewCounts.size()); i++) {
                ItemViewCount itemViewCount = itemViewCounts.get(i);
                sb.append("No. ").append(i+1)
                        .append("\titemId: ").append(itemViewCount.getItemId())
                        .append("\tviewCount: ").append(itemViewCount.getCount())
                        .append("\twindowEnd: ").append(new Timestamp(itemViewCount.getWindowEnd()))
                        .append("\n");
            }
            sb.append("=============================\n");

            Thread.sleep(1000L);
            out.collect(sb.toString());
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            itemViewCountListState.add(value);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1L);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<>("view-count-list-state", ItemViewCount.class));
        }
    }

    public static class ItemCountWindowFunc implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            Long itemId= tuple.getField(0);
            Long windowEnd = window.getEnd();
            Long count = input.iterator().next();

            out.collect(new ItemViewCount(itemId, windowEnd, count));
        }
    }

    public static class ItemCountAggFunc implements AggregateFunction<UserBehavior, Long, Long>{
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
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
