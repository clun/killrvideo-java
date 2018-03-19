package killrvideo.rest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import brave.Span;
import brave.Tracer;
import killrvideo.dao.bean.CommentListResult;
import killrvideo.dao.bean.QueryCommentByVideo;
import killrvideo.dao.dse.CommentDseDao;

/**
 * Proposal to work on Video (API).
 *
 * @author DataStax evangelist team.
 */
@RestController
public class VideosResource {
    
    @Autowired
    private CommentDseDao dseDao;
    
    @Autowired
    private Tracer tracer;
    
    @RequestMapping(method=RequestMethod.GET, value="/api/v1/videos/{videoid}/comments", produces = MediaType.APPLICATION_JSON_VALUE)
    public CommentListResult getVideoComments(@PathVariable String videoid) {
        Span span = tracer.nextSpan().name("API_VIDEOS_ListComments").start();
        try {
            span.annotate("Request received from API for " + videoid);
            return dseDao.findCommentsByVideoId(new QueryCommentByVideo(videoid));
        } finally {
            span.finish();
        }
       
    }

}
