package com.deppon.first_storm.topology;

import com.deppon.first_storm.bolt.TestWordBolt;
import com.deppon.first_storm.spout.TestWordSpout;
import com.deppon.first_storm.util.Utils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

public class TestWordTopology {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        System.out.println("=========================");
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word", new TestWordSpout(), 10);//发射一个字符如 tom
        builder.setBolt("test1",new TestWordBolt(),3).shuffleGrouping("word");//tom!!!
        builder.setBolt("test2",new TestWordBolt(),2).shuffleGrouping("test1");//tom!!!!!!

        Config config = new Config();
        config.setDebug(true);
        if(args!=null&&args.length>0){
            config.setNumWorkers(3);
            try {
                StormSubmitter.submitTopology(args[0],config,builder.createTopology());
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
        }else{
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("testWord",config,builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("testWord");
            cluster.shutdown();
        }
    }
}
