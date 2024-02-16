package br.com.actionsys.kawhyimport;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@EnableScheduling
@SpringBootApplication(scanBasePackages = "br.com.actionsys")
public class ImportNfeApplication implements ApplicationRunner {

    public static void main(String[] args) {
        SpringApplication.run(ImportNfeApplication.class, args);
    }

    @Autowired
    private Environment env;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("------------- " + env.getProperty("pom.project.name") + " " + env.getProperty("pom.project.version") + " -------------");
        log.info("Versão do java no pom : " + env.getProperty("pom.java.version") + "\n");
        log.info("-------------Banco de dados -------------");
        log.info("db.driver : " + env.getProperty("db.driver"));
        log.info("db.url : " + env.getProperty("db.url"));
        log.info("db.biblioteca : " + env.getProperty("biblioteca"));
        log.info("db.usuario : " + env.getProperty("usuario") + "\n");
        log.info("-------------KaWhyNfeConfig-------------");
        log.info("schedule.delay-seconds : " + env.getProperty("schedule.delay-seconds") + "\n");
        log.info("-------------Configurações de pastas-------------");
        log.info("dir.base : " + env.getProperty("dir.base"));
        log.info("dir.entrada : " + env.getProperty("dir.entrada"));
        log.info("dir.saida : " + env.getProperty("dir.saida"));
        log.info("dir.erro : " + env.getProperty("dir.erro"));
        log.info("dir.duplicados : " + env.getProperty("dir.duplicados"));
        log.info("dir.log : " + env.getProperty("dir.log"));
        log.info("dir.companhiascript : " + env.getProperty("dir.companhiascript"));
        log.info("-------------------------------------------------------------\n");
    }
}
