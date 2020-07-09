package com.yifan.simple.hadoop.util;

import java.util.List;

public class ListUtil {

    public static <T> void printList(List<T> list) {
        for (T t : list) {
            System.out.println(t.toString());
        }
    }
}
