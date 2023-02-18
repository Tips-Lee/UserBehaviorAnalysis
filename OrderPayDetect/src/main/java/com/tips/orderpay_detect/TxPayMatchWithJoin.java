package com.tips.orderpay_detect;

import com.tips.orderpay_detect.beans.OrderEvent;
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
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.net.URL;

public class TxPayMatchWithJoin {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        String orderPath = "/OrderLog.csv";
        String receiptPath = "/ReceiptLog.csv";
        final URL orderResource = TxPayMatchWithJoin.class.getResource(orderPath);
        final URL receiptResource = TxPayMatchWithJoin.class.getResource(receiptPath);

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

        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> outputStream = orderStream.keyBy(OrderEvent::getTxId)
                .intervalJoin(receiptStream.keyBy(ReceiptEvent::getTxId))
                .between(Time.seconds(-3), Time.seconds(5))
                .lowerBoundExclusive()
                .process(new TxPayProcJoinfunc());


        outputStream.print("success");
        env.execute("Tx match with join");
    }

    public static class TxPayProcJoinfunc extends ProcessJoinFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>> {

        @Override
        public void processElement(OrderEvent left, ReceiptEvent right, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) {
            out.collect(new Tuple2<>(left, right));
        }
    }

}
