package com.deppon.csdn_study.topology;

import com.deppon.csdn_study.bolt.PhoneTerminal2UpperCaseBolt;
import com.deppon.csdn_study.bolt.PhoneTerminalSuffix;
import com.deppon.csdn_study.spout.PhoneTerminalSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Project name storm-study
 * Package name com.deppon.csdn_study.topology
 * Description:
 * 构造一个topology对象，向集群提交程序
 *
 * Created by 326007
 * Created date 2017/8/7
 */
public class TopologySubmitClient {

    public static void main(String[] args) {
        //先获取一个topology的构造器
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        /**
         * 指定topology使用的spout组件
         * @param id 指定spout的id
         * @param spout实例
         */
        topologyBuilder.setSpout("phone_spout",new PhoneTerminalSpout());
        /**
         * 指定topology的bolt组件——用于转换手机品牌为大写
         * @param id 指定bolt的id
         * @param bolt实例
         *
         * 同时指定本bolt的消息的流向
         * 分配的方式-shuffleGroup
         * 消息的来源（从哪个组件流过来的），上面定义过spout的ID，这里直接使用id指定
         */
        topologyBuilder.setBolt("upper_phone_bolt",new PhoneTerminal2UpperCaseBolt()).shuffleGrouping("phone_spout");

        /**
         * 指定topology的第二个bolt组件（添加日期后缀的组件）
         * @param id 指定bolt的id
         * @param bolt实例
         *
         * 同时指定本bolt的消息的流向
         * 分配的方式-shuffleGroup
         * 消息的来源（从哪个组件流过来的），上面定义过bolt的ID，这里直接使用id指定
         */
        topologyBuilder.setBolt("suffix_phone_bolt",new PhoneTerminalSuffix()).shuffleGrouping("upper_phone_bolt");


         //使用builder创建一个topology对象
         StormTopology phoneTopology = topologyBuilder.createTopology();

        /**
         * 将topology提交集群运行
         * 指定config的配置信息
         * 提交集群环境运行和本地运行两种方式
         * 通过参数区分两种运行方式
         */
        Config config = new Config();
        //指定集群环境为topology分配6个worker
        config.setNumWorkers(6);

        if(args!=null&&args.length>0){
            //通过StormSubmitter向集群提交topology
            try {
                StormSubmitter.submitTopology("phone_topology",config,phoneTopology);
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
        }
        else{
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("phone_topology",config,phoneTopology);
        }
    }
}
