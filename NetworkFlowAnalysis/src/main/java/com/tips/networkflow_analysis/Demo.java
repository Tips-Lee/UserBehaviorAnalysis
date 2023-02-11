package com.tips.networkflow_analysis;

public class Demo {
    public static void main(String[] args) {
        String s = "123abcABC";
        int x = 1;
        StringBuilder sb = new StringBuilder(16);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            int y = x * c;
            sb.append(y).append(", ");
        }

        System.out.println(sb);

        int y = 3;
        System.out.println(y&4);

        char i = 3;
        System.out.println(i);

        System.out.println(Long.MAX_VALUE);
        System.out.println(Integer.MIN_VALUE);
        System.out.println(Math.pow(2, 31));
    }
}
