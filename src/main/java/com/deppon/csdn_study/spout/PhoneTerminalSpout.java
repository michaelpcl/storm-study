package com.deppon.csdn_study.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * Project name storm-study
 * Package name com.deppon.csdn_study.spout
 * Description:
 * spout组件是整个topology的源组件
 *
 * Created by 326007
 * Created date 2017/8/7
 */
public class PhoneTerminalSpout extends BaseRichSpout {
    String [] phones = {"iphone","xiaomi","yijia","google","sunsumg","huawei","meizu"};



    /**
     * spout组件初始化的方法
     * @param map
     * @param topologyContext
     * @param spoutOutputCollector
     */
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

    }

    /**
     * 消息的处理方法，不断的向后续程序发送消息，每调用一次，发送一个tuple
     * 会连续不断的被worker进程的excutor调用
     */
    @Override
    public void nextTuple() {
        Random r = new Random();
        String phone = phones[r.nextInt(phones.length)];

    }

    /**
     * 定义组件发出的tuple的schema
     * 比如：有多少个字段，字段名称等
     * @param outputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }



}
