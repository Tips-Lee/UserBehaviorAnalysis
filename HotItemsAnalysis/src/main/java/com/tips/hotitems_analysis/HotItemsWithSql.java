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
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Properties;


public class HotItemsWithSql {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //  文件数据源
        String inputPath ="./HotItemsAnalysis/src/main/resources/UserBehavior.csv";
        DataStream<String> inputStream = env.readTextFile(inputPath);

        DataStream<UserBehavior> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(Long.valueOf(fields[0]), Long.valueOf(fields[1]), Integer.valueOf(fields[2]), fields[3], Long.valueOf(fields[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000L;
            }
        });

        tableEnv.createTemporaryView("inputTable", dataStream, "userId, itemId, categoryId, behavior, timestamp as ts, rt.rowtime, pt.proctime");


        // table api
        /*Table inputTable = tableEnv.from("inputTable");
        Table windowAggTable = inputTable.filter("behavior='pv'")
                .window(Slide.over("1.hours").every("5.minutes").on("rt").as("w"))
                .groupBy("itemId, w")
                .select("itemId,  w.end as windowEnd, behavior.count as cnt");

        tableEnv.createTemporaryView("windowAggTable", tableEnv.toAppendStream(windowAggTable, Row.class), "itemId, windowEnd, cnt");

        String sql = "SELECT itemId, cnt, windowEnd, rn FROM (SELECT windowEnd, itemId, cnt, ROW_NUMBER() OVER(PARTITION BY itemId ORDER BY cnt DESC) AS rn FROM windowAggTable) t1 WHERE rn <=5 ";
        Table outputTable = tableEnv.sqlQuery(sql);*/

        // SQL
        String sql = "" +
                "SELECT itemId" +
                ", cnt" +
                ", windowEnd" +
                ", rn " +
                "FROM (" +
                "       SELECT " +
                "           windowEnd" +
                "           , itemId" +
                "           , cnt" +
                "           , ROW_NUMBER() OVER(PARTITION BY itemId ORDER BY cnt DESC) AS rn " +
                "       FROM (" +
                "           SELECT itemId" +
                "                   , HOP_END(rt, INTERVAL '5' MINUTE, INTERVAL '1' HOUR) AS windowEnd" +
                "                   , COUNT(behavior) AS cnt " +
                "           FROM inputTable " +
                "           GROUP BY itemId, HOP(rt, INTERVAL '5' MINUTE, INTERVAL '1' HOUR) " +
                "       ) t2" +
                ") t1 " +
                "WHERE t1.rn <=5 ";
        Table outputTable = tableEnv.sqlQuery(sql);

        tableEnv.toRetractStream(outputTable, Row.class).print();
        env.execute("hot items analysis");
    }

}
