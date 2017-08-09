package com.deppon.csdn_study.spout;

import com.deppon.first_storm.util.Utils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
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

    private SpoutOutputCollector collector;


    /**
     * spout组件初始化的方法
     * @param map
     * @param topologyContext
     * @param spoutOutputCollector
     */
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    /**
     * 消息的处理方法，不断的向后续程序发送消息，每调用一次，发送一个tuple
     * 会连续不断的被worker进程的excutor调用
     */
    @Override
    public void nextTuple() {
        //休眠1s在发送出去
        Utils.sleep(1000);
        Random r = new Random();
        String phone = phones[r.nextInt(phones.length)];
        //将拿到的数据封装在tuple中发送出去
        /**
         * 一个tuple中可以发送多个数据
         * 封装在list中
         * collector.emit(new values("","",""))
         */
        collector.emit(new Values(phone));
    }

    /**
     * 定义组件发出的tuple的schema
     * 比如：有多少个字段，字段名称等
     * @param outputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        /**
         * 为发出的tuple中的每个字段定义一个name
         * outputFieldsDeclarer.declare(new Fields("field1","field2","field3"));
         */
        outputFieldsDeclarer.declare(new Fields("spout_phone"));
    }



}
