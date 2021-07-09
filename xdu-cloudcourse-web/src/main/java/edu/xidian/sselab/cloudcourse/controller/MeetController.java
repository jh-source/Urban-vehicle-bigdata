package edu.xidian.sselab.cloudcourse.controller;

import edu.xidian.sselab.cloudcourse.domain.CarMeet;
import edu.xidian.sselab.cloudcourse.repository.MeetCountRespository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.List;

@Controller
@RequestMapping("/meet")
public class MeetController {
    private final MeetCountRespository respository;

    @Autowired
    public MeetController(MeetCountRespository respository) {
        this.respository = respository;
    }

    @GetMapping("")
    public String track(Model model) {
        model.addAttribute("title", "相遇统计");
        model.addAttribute("condition", new CarMeet());
        model.addAttribute("carMeetsList", new CarMeet());

        return "meet";
    }

    @PostMapping("")
    public String post(Model model, CarMeet carMeet) {
        List<CarMeet> carMeetsList = respository.findAllMeets(carMeet);

        model.addAttribute("carMeetsList", carMeetsList.isEmpty() ? new CarMeet() : carMeetsList);

        model.addAttribute("title", "相遇统计");
        model.addAttribute("condition", carMeet);
        return "meet";
    }
}
