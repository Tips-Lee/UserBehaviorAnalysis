package com.tips.orderpay_detect;

import com.tips.orderpay_detect.beans.OrderEvent;
import com.tips.orderpay_detect.beans.OrderResult;
import com.tips.orderpay_detect.beans.ReceiptEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.net.URL;

public class TxPayMatch {
    private static final OutputTag<OrderEvent> unmatchedPaysTag = new OutputTag<>("unmatchedPaysTag"){};
    private static final OutputTag<ReceiptEvent> unmatchedReceiptTag = new OutputTag<>("unmatchedReceiptTag"){};

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        String orderPath = "/OrderLog.csv";
        String receiptPath = "/ReceiptLog.csv";
        final URL orderResource = TxPayMatch.class.getResource(orderPath);
        final URL receiptResource = TxPayMatch.class.getResource(receiptPath);

        DataStream<OrderEvent> orderStream = env.readTextFile(orderResource.getPath(), "UTF-8").map(line -> {
            String[] fields = line.split(",");
            return new OrderEvent(Long.valueOf(fields[0]), fields[1], fields[2], Long.valueOf(fields[3]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<>(Time.seconds(3)) {
            @Override
            public long extractTimestamp(OrderEvent element) {
                return element.getTimestamp() * 1000L;
            }
        }).filter(x->!"".equals(x.getTxId()));

        DataStream<ReceiptEvent> receiptStream = env.readTextFile(receiptResource.getPath(), "UTF-8").map(line->{
            String[] fields = line.split(",");
            return new ReceiptEvent(fields[0], fields[1], Long.valueOf(fields[2]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReceiptEvent>() {
            @Override
            public long extractAscendingTimestamp(ReceiptEvent element) {
                return element.getTimestamp()*1000L;
            }
        });

        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> outputStream = orderStream.keyBy(OrderEvent::getTxId).connect(receiptStream.keyBy(ReceiptEvent::getTxId)).process(new TxCoProFunc());

        outputStream.print("success");
        outputStream.getSideOutput(unmatchedPaysTag).print("unmatchedPays");
        outputStream.getSideOutput(unmatchedReceiptTag).print("unmatchedReceiptTag");
        env.execute("Tx match");
    }

    public static class TxCoProFunc extends CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>> {

        ValueState<OrderEvent> orderState;
        ValueState<ReceiptEvent> receiptState;

        @Override
        public void open(Configuration parameters) {
            orderState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("order-state", OrderEvent.class));
            receiptState = getRuntimeContext().getState(new ValueStateDescriptor<ReceiptEvent>("receipt-state", ReceiptEvent.class));

        }

        @Override
        public void processElement1(OrderEvent pay, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws IOException {
            ReceiptEvent receipt = receiptState.value();
            if (receipt == null){
                ctx.timerService().registerEventTimeTimer((pay.getTimestamp() + 5L) * 1000L);
                orderState.update(pay);
            }else {
                out.collect(new Tuple2<>(pay, receipt));
                orderState.clear();
                receiptState.clear();
            }
        }

        @Override
        public void processElement2(ReceiptEvent receipt, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws IOException {
            OrderEvent pay = orderState.value();
            if (pay == null){
                ctx.timerService().registerEventTimeTimer((receipt.getTimestamp() + 3L) * 1000L);
                receiptState.update(receipt);
            }else {
                out.collect(new Tuple2<>(pay, receipt));
                orderState.clear();
                receiptState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            if (receiptState.value() != null){
                ctx.output(unmatchedReceiptTag, receiptState.value());
            }
            if (orderState.value() != null){
                ctx.output(unmatchedPaysTag, orderState.value());
            }
            orderState.clear();
            receiptState.clear();
        }
    }
}
