package killrvideo.rest;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * Have a simple
 *
 * @author DataStax evangelist team.
 */
@RestController
public class HomeResource {

    @RequestMapping(value = "/", method = RequestMethod.GET, produces = "text/html")
    public String sayHello() {
        StringBuilder response = new StringBuilder("<html><body><h1>Welcome to KillrVideo Services</h1><ul>");
        response.append("<li> Access the <b>WebConsole</b> <a href=\"./ff4j-web-console/home\">FEATURE TOGGLE WEB CONSOLE</a>");
        response.append("</span></body></html>");
        return response.toString();
    }

}
