package org.apache.activemq;

/**
 * Created by zhouxiaoling on 2016/12/13.
 */
public enum ClientEnv {

    ABT("ABT"), PRD("PRD");

    private String s;

    ClientEnv(String s) {
        this.s = s;
    }

    @Override
    public String toString() {
        return s.toUpperCase();
    }

    public static void main(String[] args) {
        System.out.println(ClientEnv.valueOf("abt".toUpperCase()));
    }

}
