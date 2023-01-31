package com.tips.networkflow_analysis.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class PageViewCount {
    private String url;
    private Long windowEnd;
    private Long cnt;
}
