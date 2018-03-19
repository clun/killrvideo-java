package killrvideo.configuration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import brave.Tracer;
import brave.Tracing;
import brave.http.HttpTracing;
import brave.propagation.B3Propagation;
import brave.propagation.ExtraFieldPropagation;
import brave.spring.web.TracingClientHttpRequestInterceptor;
import brave.spring.webmvc.SpanCustomizingAsyncHandlerInterceptor;
import zipkin2.Span;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.Reporter;
import zipkin2.reporter.okhttp3.OkHttpSender;

/**
 * Implementation of distributed tracing using Brave and ZipKin:
 * - SpringCloud Sleuth is not ready for SpringBoot 2.0 and hide to many things
 * - We would like to trace both GRPC and HTTP services.
 *
 * @author DataStax evangelist team.
 */
@Configuration
@EnableWebMvc
@Import({
    TracingClientHttpRequestInterceptor.class,
    SpanCustomizingAsyncHandlerInterceptor.class
})
public class TracingConfiguration implements WebMvcConfigurer {
    
    /** Initialize dedicated connection to ETCD system. */
    private static final Logger LOGGER = LoggerFactory.getLogger(TracingConfiguration.class);
   
    @Value("${tracing.zipkin.url: 'http://127.0.0.1:9411/api/v2/spans'}")
    private String zipkinUrl;
    
    @Value("${tracing.zipkin.enabled}")
    private boolean zipkinEnabled;
    
    @Value("${tracing.serviceName: 'killrVideo'}")
    private String serviceName;
    
    /** Build-In Wrapper for EXPOSIING REST CONTROLLERS. */
    @Autowired 
    private SpanCustomizingAsyncHandlerInterceptor serverInterceptor;
    
    /** Send information to Zipkinm could be AMQP, KAFKA or HTTP depending on dependencies. */
    private OkHttpSender sender;
    
    /** Span reporter. */
    private Reporter<Span> spanReporter;
    
    /** Create. */
    private Tracing tracing;
    
    @Bean
    public OkHttpSender sender() {
        LOGGER.info("Initializing Tracing to {}", zipkinUrl);
        this.sender = OkHttpSender.create(zipkinUrl);
        return sender;
    }
    
    @Bean
    @ConditionalOnMissingBean
    public Reporter<Span> spanReporter(OkHttpSender sender) {
        if (zipkinEnabled) {
            this.spanReporter = AsyncReporter.create(sender);
        } else {
            this.spanReporter = Reporter.CONSOLE;
        }
        return spanReporter;
    }
    
    @Bean
    @ConditionalOnMissingBean
    public Tracing createTracing(Reporter<Span> spanReporter) {
       this.tracing = Tracing.newBuilder()
                .localServiceName(serviceName)
                .propagationFactory(ExtraFieldPropagation.newFactory(B3Propagation.FACTORY, "user-name"))
                .spanReporter(spanReporter)
                .build();
        return this.tracing;
    }
    
    @Bean 
    public HttpTracing httpTracing(Tracing tracing) {
      return HttpTracing.create(tracing);
    }
       
    @Bean
    @ConditionalOnMissingBean
    public Tracer createTracer(Tracing tracing) {
        return tracing.tracer();
    }
    
     /** 
      * Add Spring MVC lifecycle interceptors for pre-and post-processing 
      * of controller method invocations. 
      **/
    @Override 
    public void addInterceptors(InterceptorRegistry registry) {
      LOGGER.info("Initializing Interceptor for Spring MVC with {}");
      registry.addInterceptor(serverInterceptor);
    }
    
    @PostConstruct
    public void prepare() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() { stopTracing(); }
        });
    }
    
    /**
     * Properly close the Span
     */
    @PreDestroy
    public void stopTracing() {
        LOGGER.info("Stopping Tracer...");
        tracing.close();
        if (spanReporter instanceof AsyncReporter) {
            ((AsyncReporter<Span>)spanReporter).close();
        }
        sender.close();
    }

}
