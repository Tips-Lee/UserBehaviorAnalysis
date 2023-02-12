package com.tips.login_fail_detect;

import com.tips.login_fail_detect.beans.LoginEvent;
import com.tips.login_fail_detect.beans.LoginFailWarning;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;
import java.util.List;
import java.util.Map;

public class LoginFailWithCepV1 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        String inputPath = "/LoginLog.csv";
        final URL resource = LoginFailWithCepV1.class.getResource(inputPath);

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

        // 1. 创建pattern
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("firstFail").where(new SimpleCondition<>() {
            @Override
            public boolean filter(LoginEvent value) {
                return "fail".equals(value.getStatus());
            }
        }).next("secondFail").where(new SimpleCondition<>() {
            @Override
            public boolean filter(LoginEvent value) {
                return "fail".equals(value.getStatus());
            }
        }).next("lastFail").where(new SimpleCondition<>() {
            @Override
            public boolean filter(LoginEvent value) {
                return "fail".equals(value.getStatus());
            }
        }).within(Time.seconds(3));

        // 2. 应用pattern
        PatternStream<LoginEvent> patternStream = CEP.pattern(dataStream.keyBy(LoginEvent::getUserId), pattern);

        // 3 选择pattern
        SingleOutputStreamOperator<LoginFailWarning> outputStream = patternStream.select(new PatternSelectFunction<LoginEvent, LoginFailWarning>() {
            @Override
            public LoginFailWarning select(Map<String, List<LoginEvent>> map) {
                LoginEvent firstFail = map.get("firstFail").iterator().next();
                LoginEvent secondFail = map.get("lastFail").get(0);
                return new LoginFailWarning(firstFail.getUserId(), firstFail.getTimestamp(), secondFail.getTimestamp(), "login failed 3 times");
            }
        });

        outputStream.print();
        env.execute("login fail detect with cep");
    }
}
