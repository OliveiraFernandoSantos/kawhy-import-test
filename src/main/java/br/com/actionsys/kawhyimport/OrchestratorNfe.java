package br.com.actionsys.kawhyimport;

import br.com.actionsys.kawhycommons.integration.IntegrationItem;
import br.com.actionsys.kawhyimport.command.SqlCommand;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class OrchestratorNfe extends ImportOrchestrator {


    @Override
    public void processDocumentFile(IntegrationItem item) throws Exception {
        super.processDocumentFile(item);
    }

  @Override
  public List<SqlCommand> generateControlCommands(IntegrationItem item) throws Exception {
    return null;
  }

  @Override
    public void processCancelFile(IntegrationItem item) throws Exception {

    }

    @Override
    public void processEventFile(IntegrationItem item) throws Exception {

    }

    @Override
    public boolean isCancel(IntegrationItem item) throws Exception {
        return false;
    }

    @Override
    public boolean isEvent(IntegrationItem item) throws Exception {
        return false;
    }

    @Override
    public boolean isDocument(IntegrationItem item) throws Exception {
        return true;
    }

  @Override
  public void processCancelFile(IntegrationItem item) throws Exception {

  }
}
