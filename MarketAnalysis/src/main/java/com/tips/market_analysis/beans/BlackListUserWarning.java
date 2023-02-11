package com.tips.market_analysis.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class BlackListUserWarning {
    private Long userId;
    private Long adId;
    private String warningMsg;
}
