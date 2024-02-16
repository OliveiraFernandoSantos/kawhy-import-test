package br.com.actionsys.kawhyimport;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class Schedule {
  @Autowired
  private OrchestratorNfe orchestrator;

  @Scheduled(fixedDelayString = "#{${schedule.delay-seconds} * 1000}")
  public void process() {
    orchestrator.process();
  }

}
