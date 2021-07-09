package edu.xidian.sselab.cloudcourse.controller;

import edu.xidian.sselab.cloudcourse.domain.Record;
import edu.xidian.sselab.cloudcourse.repository.FliterRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.List;

@Controller
@RequestMapping("/fliter")
public class FliterController {

    private final FliterRepository repository;

    @Autowired
    public FliterController(FliterRepository repository) {
        this.repository = repository;
    }

    @GetMapping("")
    public String get(Model model) {
        List<Record> recordList = repository.findAllByRecord(new Record());
        model.addAttribute("title", "非法数据");
        model.addAttribute("condition", new Record());
        model.addAttribute("recordList", recordList);
        return "fliter";
    }

    @PostMapping("")
    public String post(Model model, Record record) {
        List<Record> recordList = repository.findAllByRecord(record);
        model.addAttribute("recordList", recordList);
        model.addAttribute("title", "非法数据");
        model.addAttribute("condition", record);
        return "fliter";
    }
}
