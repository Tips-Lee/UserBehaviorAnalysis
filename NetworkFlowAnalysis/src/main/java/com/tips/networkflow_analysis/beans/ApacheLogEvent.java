package com.tips.networkflow_analysis.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Data
@AllArgsConstructor
public class ApacheLogEvent {
    private String ip;
    private String userId;
    private Long timestamp;
    private String method;
    private String url;
}
