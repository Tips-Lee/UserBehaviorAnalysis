package com.tips.orderpay_detect;

import com.tips.orderpay_detect.beans.OrderEvent;
import com.tips.orderpay_detect.beans.OrderResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;

public class OrderPayTimeoutV1 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        String inputPath = "/OrderLog.csv";
        final URL resource = OrderPayTimeoutV1.class.getResource(inputPath);

        DataStream<String> inputStream = env.readTextFile(resource.getPath(), "UTF-8");
        DataStream<OrderEvent> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new OrderEvent(Long.valueOf(fields[0]), fields[1], fields[2], Long.valueOf(fields[3]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<>(Time.seconds(3)) {
            @Override
            public long extractTimestamp(OrderEvent element) {
                return element.getTimestamp() * 1000L;
            }
        });

        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("create").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "create".equals(value.getEventType());
            }
        }).followedBy("pay").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) {
                return "pay".equals(value.getEventType());
            }
        }).within(Time.minutes(15));

        PatternStream<OrderEvent> patternStream = CEP.pattern(dataStream.keyBy(OrderEvent::getOrderId), pattern);

        OutputTag<OrderResult> tag = new OutputTag<>("timeout") {};
        SingleOutputStreamOperator<OrderResult> outputStream = patternStream.select(tag, new OrderTimeoutFunc(), new OrderSelectFunc());

        outputStream.print("success");
        outputStream.getSideOutput(tag).print("timeout");
        env.execute("order timeout detect with cep");
    }

    public static class OrderTimeoutFunc implements PatternTimeoutFunction<OrderEvent, OrderResult>{
        @Override
        public OrderResult timeout(Map<String, List<OrderEvent>> map, long timestamp) {
            Long orderId = map.get("create").iterator().next().getOrderId();
            return new OrderResult(orderId, "order payment timeout: " + timestamp);
        }
    }

    public static class OrderSelectFunc implements PatternSelectFunction<OrderEvent, OrderResult>{

        @Override
        public OrderResult select(Map<String, List<OrderEvent>> map) throws Exception {
            Long orderId = map.get("create").iterator().next().getOrderId();
            return new OrderResult(orderId, "order payment success");
        }
    }

}
