package killrvideo;

import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;

/**
 * Main class to execute killrvideo.
 *
 * @author DataStax evangelist team.
 */
@EnableAutoConfiguration
@ComponentScan
public class KillrVideoServer {

    /**
     * As SpringBoot application, this is the "main" class
     */
    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(KillrVideoServer.class);
        app.setBannerMode(Banner.Mode.OFF);
        app.run(args);
    }
    
}