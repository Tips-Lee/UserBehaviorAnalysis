package com.tips.hotitems_analysis.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ItemViewCount {
    private Long itemId;
    private Long windowEnd;
    private Long count;
}
