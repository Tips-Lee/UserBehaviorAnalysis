package com.tips.orderpay_detect;

import com.tips.orderpay_detect.beans.OrderEvent;
import com.tips.orderpay_detect.beans.OrderResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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

public class OrderPayTimeoutV2 {
    private static OutputTag<OrderResult> tag = new OutputTag<>("timeout") {};
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        String inputPath = "/OrderLog.csv";
        final URL resource = OrderPayTimeoutV2.class.getResource(inputPath);

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
        SingleOutputStreamOperator<OrderResult> outputStream = dataStream.keyBy(OrderEvent::getOrderId).process(new OrderProcessFunc());


        outputStream.print("success");
        outputStream.getSideOutput(tag).print("timeout");
        env.execute("order timeout detect without cep");
    }


    public static class OrderProcessFunc extends KeyedProcessFunction<Long, OrderEvent, OrderResult>{
        ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) {
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-state", Long.class));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderResult> out) {
            Long orderId = ctx.getCurrentKey();
            ctx.output(tag, new OrderResult(orderId, "order payment timeout: " + timestamp));
            timerState.clear();
        }

        @Override
        public void processElement(OrderEvent value, Context ctx, Collector<OrderResult> out) throws IOException {
            if ("create".equals(value.getEventType())){
                long timer = ctx.timestamp() + 15 * 60 * 1000L;
                ctx.timerService().registerEventTimeTimer(timer);
                timerState.update(timer);

            }else {
                Long orderId = ctx.getCurrentKey();
                out.collect(new OrderResult(orderId, "order payment success"));
                if (timerState.value() != null){
                    ctx.timerService().deleteEventTimeTimer(timerState.value());
                    timerState.clear();
                }
            }
        }
    }


}
