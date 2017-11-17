package com.kgc.hadoop.hdfs;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Date;

/**
 * Created by kgc on 2017/11/17.
 */
public class LogGenerator {
    public static void main(String[] args) throws Exception{
        Logger logger = LogManager.getLogger("LogGenerator");
        int i = 0;

        while(true){
            logger.info("~~~~~~~~~~~~" + new Date().toString() + "~~~~~~~~~~~~");
            i++;
            Thread.sleep(500);
            if(i > 1000000)break;
        }
    }
}
