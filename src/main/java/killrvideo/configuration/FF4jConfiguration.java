package killrvideo.configuration;

import org.ff4j.FF4j;
import org.ff4j.web.FF4jDispatcherServlet;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Enabling feature toggle.
 *
 * @author DataStax evangelist team.
 */
@Configuration
public class FF4jConfiguration {

    /** 
     * Define Feature Toggle: So far there is no permanent store. Configuration is load
     * from XML and hold in memory. To persist states define here the store you like, probable
     * Cassandra here.
     **/
    @Bean
    public FF4j getFF4j() {
        FF4j ff4j = new FF4j("ff4j-conf.xml");
        ff4j.audit(true);
        ff4j.autoCreate(true);
        return ff4j;
    }
    
    /** Define Feature Toggle UI. */
    @Bean
    @ConditionalOnMissingBean
    public FF4jDispatcherServlet getFF4jDispatcherServlet(FF4j ff4j) {
        FF4jDispatcherServlet ff4jConsoleServlet = new FF4jDispatcherServlet();
        ff4jConsoleServlet.setFf4j(ff4j);
        return ff4jConsoleServlet;
    }
    
    /** Register Servlet for UI in Spring-Boot */
    @Bean
    public ServletRegistrationBean<FF4jDispatcherServlet> ff4jDispatcherServletRegistrationBean(FF4jDispatcherServlet ff4jDispatcherServlet) {
        return new ServletRegistrationBean<FF4jDispatcherServlet>(ff4jDispatcherServlet, "/ff4j-web-console/*");
    }
    
}
