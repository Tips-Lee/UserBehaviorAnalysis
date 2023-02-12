package com.tips.login_fail_detect;

import com.tips.login_fail_detect.beans.LoginEvent;
import com.tips.login_fail_detect.beans.LoginFailWarning;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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

import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;

public class LoginFailV2 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        String inputPath = "/LoginLog.csv";
        final URL resource = LoginFailV2.class.getResource(inputPath);

        DataStream<String> inputStream = env.readTextFile(resource.getPath(), "UTF-8");
        DataStream<LoginEvent> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new LoginEvent(Long.valueOf(fields[0]), fields[1], fields[2], Long.valueOf(fields[3]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<>(Time.seconds(3)) {
            @Override
            public long extractTimestamp(LoginEvent element) {
                return element.getTimestamp() * 1000L;
            }
        });

        SingleOutputStreamOperator<LoginFailWarning> outputStream = dataStream.keyBy(LoginEvent::getUserId)
                .process(new LoginWarningKeyedProcessFunction());

        outputStream.print();
        env.execute();
    }

    public static class LoginWarningKeyedProcessFunction extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {

        ListState<LoginEvent> loginFailEventListState;
        @Override
        public void open(Configuration parameters) throws Exception {
            loginFailEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("login-fail-list", LoginEvent.class));
        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<LoginFailWarning> out) throws Exception {
            if ("fail".equals(value.getStatus())){
                Iterator<LoginEvent> iterator = loginFailEventListState.get().iterator();
                if (iterator.hasNext()){
                    LoginEvent lastFail = iterator.next();
                    out.collect(new LoginFailWarning(ctx.getCurrentKey(), lastFail.getTimestamp(), value.getTimestamp(), "login failed for 2 times"));
                }

                loginFailEventListState.clear();
                loginFailEventListState.add(value);
            }else {
                loginFailEventListState.clear();
            }
        }
    }
}
