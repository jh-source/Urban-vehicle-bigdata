package edu.xidian.sselab.cloudcourse.controller;

import java.util.List;


import edu.xidian.sselab.cloudcourse.domain.Record;
import edu.xidian.sselab.cloudcourse.hbase.HbaseClient;
import edu.xidian.sselab.cloudcourse.hbase.getTrace;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import edu.xidian.sselab.cloudcourse.domain.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

@Controller
@RequestMapping("/track")
public class TrackController {
    @GetMapping("")
    public String get(Model model) {
        model.addAttribute("title", "轨迹重现");
        model.addAttribute("EID", new String());
        model.addAttribute("STime",new String());
        model.addAttribute("ETime",new String());
        return "track";
    }

    @PostMapping("")
    public String post(Model model, String eid,String stime,String etime) throws IOException {

        //Long stime = track.getTime();
        //Long etime = track.getTime();
        //1. 获取连接
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hbase1:2181,hbase2:2181,Spark:2181");
        Connection con = ConnectionFactory.createConnection(conf);
        //2.获取一张表
        Table table = con.getTable(TableName.valueOf("Trace"));
        List<Record> rd = getTrace.testGet(eid,stime,etime);

        model.addAttribute("tracklist",rd);
        //6.关闭连接
        con.close();
        model.addAttribute("title", "轨迹重现");

        return "track";
    }


}

