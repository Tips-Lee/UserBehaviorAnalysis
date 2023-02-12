package com.tips.login_fail_detect.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LoginFailWarning {
    private Long userId;
    private Long fistFailTime;
    private Long lastFailTime;
    private String warningMsg;
}
