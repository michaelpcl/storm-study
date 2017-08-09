package com.deppon.csdn_study.bolt;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

/**
 * Project name storm-study
 * Package name com.deppon.csdn_study.bolt
 * Description:
 * 为手机名称添加日期后缀的bolt，并输出到文件
 * Created by 326007
 * Created date 2017/8/7
 */
public class PhoneTerminalSuffix extends BaseBasicBolt{
    //定义日期格式化的方式
    private SimpleDateFormat sdf;
    //文件定义
    private FileWriter fileWriter;

    /**
     * 在bolt组件初始化的时候调用
     * 初始化 bolt所需要的对象
     *
     * @param stormConf
     * @param context
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        //初始化日期格式化的对象
        sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

        //初始化文件对象
        try {
            fileWriter = new FileWriter("E:\\storm-output\\" + UUID.randomUUID());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 因为本bolt是整个topology的最后一个bolt,需要输出结果
     * 这里为了方便测试，直接输出到文件中
     * @param tuple
     * @param basicOutputCollector
     */
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        //从tuple中获取上一个组件传递过来的数据
        String upperCasePhone = tuple.getStringByField("upper_phone");
        //业务逻辑，添加后缀
        String result = upperCasePhone  + sdf.format(new Date());
        //将结果写入到文件中
        try {
            fileWriter.write(result + "\n");
            fileWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 因为这个bolt是最后一个组件，所以不需要向后面组件传递消息
     * 因此，我们可以不用声明tuple的字段名称
     * @param outputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
