package com.deppon.csdn_study.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Project name storm-study
 * Package name com.deppon.csdn_study.bolt
 * Description:
 * 将spout发送来的手机品牌转换为大写
 *
 * Created by 326007
 * Created date 2017/8/7
 */
public class PhoneTerminal2UpperCaseBolt extends BaseBasicBolt{

    private BasicOutputCollector collector;


    /**
     * 是bolt业务逻辑的实现，他被worker进程的executor线程调用，每收到一个消息，executor就被调用一次
     * tuple:上一个组件发送过来的消息
     * basicOutputCollector 用来发射消息的工具
     * @param tuple
     */
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        this.collector = basicOutputCollector;
        /**
         * 从tuple中获取上一个组件spout中传递过来的数据
         * 获取数据的方式：
         * 方式一：通过field名称获取
         * 方式二：通过value在tuple中的脚标获取
         */
        //String phone = tuple.getStringByField("spout_phone");
        String phone = tuple.getString(0);
        //处理逻辑，将手机品牌变成大写
        String upperCasePhone = phone.toUpperCase();
        //将大写的手机品牌发射出去
        collector.emit(new Values(upperCasePhone));
    }



    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //声明本组件发出的消息的schema
        outputFieldsDeclarer.declare(new Fields("upper_phone"));
    }

}
