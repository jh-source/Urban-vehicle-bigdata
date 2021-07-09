package edu.xidian.sselab.cloudcourse.controller;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import edu.xidian.sselab.cloudcourse.domain.Record;
import edu.xidian.sselab.cloudcourse.domain.VehicleCount;

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
@RequestMapping("/count")
public class VehicleCountController {
    @GetMapping("")
    public String get(Model model) {
        model.addAttribute("title", "同地点过车");
        model.addAttribute("PlaceId", new String());
        return "count";
    }
    @PostMapping("")
    public String post(Model model,String placeId) throws IOException, JSONException {
        //1. 获取连接
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "Spark:2181,hbase1:2181,hbase2:2181");
        Connection con = ConnectionFactory.createConnection(conf);
        //2.获取一张表
        Table table = con.getTable(TableName.valueOf("VehicleCount"));
        Get get = new Get(Bytes.toBytes(placeId));
        Result result = table.get(get);
        VehicleCount vc=new VehicleCount();
        vc=vc.mapFrom(result);
        List<VehicleCount> list = new ArrayList<VehicleCount>();
        list.add(vc);
        table.close();
        model.addAttribute("countList",list);
        return "count";
    }
}
