package com.yifan.simple.hadoop.common;

import com.yifan.simple.hadoop.inputformat.MapInputFormat;
import com.yifan.simple.hadoop.inputformat.ReducerInputFormat;
import com.yifan.simple.hadoop.mapper.Mapper;
import com.yifan.simple.hadoop.outputformat.MapOutputFormat;
import com.yifan.simple.hadoop.outputformat.ReducerOutputFormat;
import com.yifan.simple.hadoop.partitioner.Partitioner;
import com.yifan.simple.hadoop.reducer.Reducer;
import com.yifan.simple.hadoop.sort.Sort;
import com.yifan.simple.hadoop.util.PropertiesUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class Context {

    private String jobID;

    private Class<MapInputFormat> mapInputFormat;
    private Class<Mapper> mapper;
    private Class<MapOutputFormat> mapOutputFormat;
    private Class<Partitioner> partitioner;
    private Class<Sort> sort;
    private Class<Reducer> reducer;
    private Class<ReducerInputFormat> reducerInputFormat;
    private Class<ReducerOutputFormat> reducerOutputFormat;

    private String inputDir;
    private String outputDir;

    private Properties properties;

    private List<String> mapTaskTempDirs;

    // 分区个数管理
    private int numPartitions;

    public Context() {
        this.properties = PropertiesUtil.loadConf();
        this.mapTaskTempDirs = new ArrayList<>();
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public List<String> getMapTaskTempDirs() {
        return mapTaskTempDirs;
    }

    public void setMapTaskTempDirs(List<String> mapTaskTempDirs) {
        this.mapTaskTempDirs = mapTaskTempDirs;
    }

    public String getConf(String key) {
        return properties.getProperty(key);
    }

    public void setConf(String key, String value) {
        properties.setProperty(key, value);
    }

    public String getJobID() {
        return jobID;
    }

    public void setJobID(String jobID) {
        this.jobID = jobID;
    }

    public Class<MapInputFormat> getMapInputFormat() {
        return mapInputFormat;
    }

    public void setMapInputFormat(Class<MapInputFormat> mapInputFormat) {
        this.mapInputFormat = mapInputFormat;
    }

    public Class<Mapper> getMapper() {
        return mapper;
    }

    public void setMapper(Class<Mapper> mapper) {
        this.mapper = mapper;
    }

    public Class<MapOutputFormat> getMapOutputFormat() {
        return mapOutputFormat;
    }

    public void setMapOutputFormat(Class<MapOutputFormat> mapOutputFormat) {
        this.mapOutputFormat = mapOutputFormat;
    }

    public Class<Partitioner> getPartitioner() {
        return partitioner;
    }

    public void setPartitioner(Class<Partitioner> partitioner) {
        this.partitioner = partitioner;
    }

    public Class<Sort> getSort() {
        return sort;
    }

    public void setSort(Class<Sort> sort) {
        this.sort = sort;
    }

    public Class<Reducer> getReducer() {
        return reducer;
    }

    public void setReducer(Class<Reducer> reducer) {
        this.reducer = reducer;
    }

    public Class<ReducerInputFormat> getReducerInputFormat() {
        return reducerInputFormat;
    }

    public void setReducerInputFormat(Class<ReducerInputFormat> reducerInputFormat) {
        this.reducerInputFormat = reducerInputFormat;
    }

    public Class<ReducerOutputFormat> getReducerOutputFormat() {
        return reducerOutputFormat;
    }

    public void setReducerOutputFormat(Class<ReducerOutputFormat> reducerOutputFormat) {
        this.reducerOutputFormat = reducerOutputFormat;
    }

    public String getInputDir() {
        return inputDir;
    }

    public void setInputDir(String inputDir) {
        this.inputDir = inputDir;
    }

    public String getOutputDir() {
        return outputDir;
    }

    public void setOutputDir(String outputDir) {
        this.outputDir = outputDir;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }
}
