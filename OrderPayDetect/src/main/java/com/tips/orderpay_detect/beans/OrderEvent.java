package com.tips.orderpay_detect.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class OrderEvent {
    private Long orderId;
    private String eventType;
    private String txId;
    private Long timestamp;
}
