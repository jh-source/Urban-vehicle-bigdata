package edu.xidian.sselab.cloudcourse.controller;

import edu.xidian.sselab.cloudcourse.domain.GetTrack;
import edu.xidian.sselab.cloudcourse.domain.Record;
import edu.xidian.sselab.cloudcourse.repository.TrackRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.List;

@Controller
@RequestMapping("/bmap")
public class MapController {
    private final TrackRepository repository;

    @Autowired
    public MapController(TrackRepository repository) {
        this.repository = repository;
    }

    @GetMapping("")
    public String get(Model model) {
        List<Record> recordList = repository.findAllByRecord(new GetTrack()).subList(0,10);
        model.addAttribute("recordList", recordList);
        model.addAttribute("title", "轨迹重现");
        model.addAttribute("condition", new GetTrack());
        return "bmap";
    }

    @PostMapping("")
    public String post(Model model, GetTrack test) {
        List<Record> recordList = repository.findAllByRecord(test).subList(0,10);
        model.addAttribute("recordList", recordList);
        model.addAttribute("title", "轨迹重现");
        model.addAttribute("condition", test);
        return "bmap";
    }
}
