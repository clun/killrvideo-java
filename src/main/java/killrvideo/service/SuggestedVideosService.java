package killrvideo.service;

import static java.util.stream.Collectors.toMap;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.annotation.PostConstruct;
import javax.inject.Inject;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.dse.DseSession;
import com.datastax.driver.dse.graph.*;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.google.gson.Gson;
import killrvideo.entity.*;
import killrvideo.utils.TypeConverter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.map.HashedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

//import info.archinnov.achilles.generated.manager.VideoByTag_Manager;
//import info.archinnov.achilles.generated.manager.Video_Manager;
import com.datastax.driver.mapping.Mapper;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import killrvideo.common.CommonTypes.Uuid;
import killrvideo.suggested_videos.SuggestedVideoServiceGrpc.AbstractSuggestedVideoService;
import killrvideo.suggested_videos.SuggestedVideosService.*;
import killrvideo.validation.KillrVideoInputValidator;

@Service
public class SuggestedVideosService extends AbstractSuggestedVideoService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SuggestedVideosService.class);

    private static final String RECOMMENDATION_SCRIPT = "" +
                    "def numRatingsToSample = 100\n" +
                    "def localUserRatingsToSample = 10\n" +
                    "\n" +
                    "// TODO: \"rated\" should become \"viewed\"\n" +
                    "def watchedMovies = g.V('{~label=user, userid=%s}').out('rated').store('x').cap('x').next()\n" +
                    "\n" +
                    "def recommendedVideos = \n" +
                    "    // For the video's I rated highly...\n" +
                    "    g.V('{~label=user, userid=%s}').outE('rated').has('rating', gte(4)).inV() // (withoutStrategies is to avoid what is probably a DSEGraph bug)\n" +
                    "    // what other users rated those videos highly? (this is like saying \"what users share my taste\")\n" +
                    "    .inE('rated').has('rating', gte(4))\n" +
                    "    // but don't grab too many, or this won't work OLTP, and \"by('rating')\" favors the higher ratings\n" +
                    "    .sample(numRatingsToSample).by('rating').outV()\n" +
                    "    // (except me of course)\n" +
                    "    .has(id, neq('{~label=user, userid=%s}'))\n" +
                    "    // Now we're working with \"similar users\". For those users who share my taste, grab N highly rated videos. Save the rating so we can sum the scores later, and use sack() because it does not require path information. (as()/select() was slow)\n" +
                    "    .local(outE('rated').has('rating', gte(4)).limit(localUserRatingsToSample)).sack(assign).by('rating').inV()\n" +
                    "     // excluding the videos I have already watched\n" +
                    "    .not(is(within(watchedMovies)))\n" +
                    "    // what are the most popular videos as calculated by the sum of all their ratings\n" +
                    "    .group().by().by(sack().sum())\n" +
                    "    // now that we have that big map of [video: score], lets order it\n" +
                    "    .order(local).by(values, decr).limit(local, 100).select(keys).unfold()";

    public static final int RELATED_VIDEOS_TO_RETURN = 4;

    @Inject
    Mapper<Video> videoMapper;

    @Inject
    Mapper<VideoByTag> videoByTagMapper;

    @Inject
    MappingManager manager;

    @Inject
    KillrVideoInputValidator validator;

    DseSession session;
    private String videoByTagTableName;

    @PostConstruct
    public void init(){
        this.session = (DseSession) manager.getSession();

        videoByTagTableName = videoByTagMapper.getTableMetadata().getName();
    }

    @Override
    public void getRelatedVideos(GetRelatedVideosRequest request, StreamObserver<GetRelatedVideosResponse> responseObserver) {

        LOGGER.debug("Start getting related videos");

        if (!validator.isValid(request, responseObserver)) {
            return;
        }

        final UUID videoId = UUID.fromString(request.getVideoId().getValue());

        final GetRelatedVideosResponse.Builder builder = GetRelatedVideosResponse.newBuilder();
        builder.setVideoId(request.getVideoId());

        /**
         * Load the source video
         */
//        final Video video = videoManager
//                .dsl()
//                .select()
//                .allColumns_FromBaseTable()
//                .where()
//                .videoid().Eq(videoId)
//                .getOne();

        final Video video = videoMapper.get(videoId);

        if (video == null) {
            returnNoResult(responseObserver, builder);
        } else {

            /**
             * Return immediately if the source video has
             * no tags
             */
            if (CollectionUtils.isEmpty(video.getTags())) {
                returnNoResult(responseObserver, builder);
            } else {
                final List<String> tags = new ArrayList<>(video.getTags());
                Map<Uuid, SuggestedVideoPreview> results = new HashedMap();

                /**
                 * Use the number of results we ultimately want * 2 when querying so that we can account
                 * for potentially having to filter out the video Id we're using as the basis for the query
                 * as well as duplicates
                 **/
                final int pageSize = RELATED_VIDEOS_TO_RETURN * 2;

                //final List<CompletableFuture<List<VideoByTag>>> inFlightQueries = new ArrayList<>();
                final List<ResultSetFuture> inFlightQueries = new ArrayList<>();

                /** Kick off a query for each tag and track them in the inflight requests list **/
                for (int i = 0; i < tags.size(); i++) {
                    String tag = tags.get(i);

                    ResultSetFuture listAsync;

//                    final CompletableFuture<List<VideoByTag>> listAsync = videoByTagManager
//                            .dsl()
//                            .select()
//                            .allColumns_FromBaseTable()
//                            .where()
//                            .tag().Eq(tag)
//                            .withFetchSize(pageSize)
//                            .getListAsync();

                    BuiltStatement statement = QueryBuilder
                            .select().all()
                            .from(Schema.KEYSPACE, videoByTagTableName)
                            .where(QueryBuilder.eq("tag", tag));

                    statement
                            .setFetchSize(pageSize);

                    listAsync = session.executeAsync(statement);
                    //FutureUtils.buildCompletableFuture(listAsync)
                    inFlightQueries.add(listAsync);

                    /** Every third query, or if this is the last tag, wait on all the query results **/
                    if (inFlightQueries.size() == 3 || i == tags.size() - 1) {

                        //for (CompletableFuture<List<VideoByTag>> future : inFlightQueries) {
                        for (ResultSetFuture future : inFlightQueries) {
                            //ResultSet futureResults = future.getUninterruptibly();
                            //List<Row> rows = futureResult.all();

                            try {
                                Result<VideoByTag> videos = videoByTagMapper.map(future.getUninterruptibly());
                                results.putAll(videos.all()
                                        .stream()
                                        .map(VideoByTag::toSuggestedVideoPreview)
                                        .filter(previewVid ->
                                                !previewVid.getVideoId().equals(request.getVideoId())
                                                && !results.containsKey(previewVid.getVideoId())
                                        )
                                        .collect(toMap(preview -> preview.getVideoId(), preview -> preview)));

                            } catch (Exception e) {
                                responseObserver.onError(Status.INTERNAL.withCause(e).asRuntimeException());
                            }
                        }

                        if (results.size() >= RELATED_VIDEOS_TO_RETURN) {
                            break;
                        } else {
                            inFlightQueries.clear();
                        }
                    }
                }
                builder.addAllVideos(results.values());
                responseObserver.onNext(builder.build());
                responseObserver.onCompleted();

                LOGGER.debug(String.format("End getting related videos with %s videos", results.size()));
            }
        }
    }

    private void returnNoResult(StreamObserver<GetRelatedVideosResponse> responseObserver, GetRelatedVideosResponse.Builder builder) {
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();

        LOGGER.debug("End getting related videos with 0 video");

    }

    @Override
    public void getSuggestedForUser(GetSuggestedForUserRequest request, StreamObserver<GetSuggestedForUserResponse> responseObserver) {

        LOGGER.debug("Start getting suggested videos for user");

        final GetSuggestedForUserResponse.Builder builder = GetSuggestedForUserResponse
                .newBuilder();
        builder.setUserId(request.getUserId());

        // Add suggested videos...
        builder.addAllVideos(getRecommendedVideos(request.getUserId()));
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();

        LOGGER.debug("End getting suggested videos for user");
    }

    private List<SuggestedVideoPreview> getRecommendedVideos(Uuid userId) {
        List<SuggestedVideoPreview> result = new ArrayList<>();

        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        String userIdString = "7da8d9b8-60bf-4aa1-906e-713b7603f3c1";
        // TODO:
        // String userIdString = userId.getValue()

        String queryString = String.format(RECOMMENDATION_SCRIPT, userIdString, userIdString, userIdString);
        GraphStatement statement = new SimpleGraphStatement(queryString)
                .setGraphName("killrvideo_graph");

        GraphResultSet videoIds = session.executeGraph(statement);
        if (videoIds != null) {
            for (GraphNode n : videoIds) {
                Vertex v = n.asVertex();

                // Extract the videoid; the id of the video vertex is a json blob containing the videoid attribute.
                Gson g = new Gson();
                VideoVertexId videoVertexId = g.fromJson(v.getId().toString(), VideoVertexId.class);
                String videoId = videoVertexId.getVideoId();
                try {
                    // The date string is in Zulu time, but ends in 'Z', which isn't a valid timezone specification.
                    // Substitute with +0000 to make the date parser happy.
                    String addedDateString = v.getProperty("added_date").getValue().asString().replace("Z", "+0000");
                    result.add(
                        SuggestedVideoPreview.newBuilder()
                                .setAddedDate(TypeConverter.dateToTimestamp(dateFormat.parse(addedDateString)))
                                .setName(v.getProperty("name").getValue().asString())
                                .setPreviewImageLocation(v.getProperty("preview_image_location").getValue().asString())
                                .setUserId(TypeConverter.uuidToUuid(UUID.fromString("966a83aa-6d8b-41b6-b4ac-79867de3b4fa")))
                                // TODO: use the following user-id instead of the static value above.
                                //.setUserId(TypeConverter.uuidToUuid(UUID.fromString(v.getProperty("userid").getValue().asString())))
                                .setVideoId(TypeConverter.uuidToUuid(UUID.fromString(videoId)))
                                .build()
                    );
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        }
        return result;
    }

    /**
     * Class used in conjunction with GSon to parse a video vertex id
     */
    private class VideoVertexId {
        private String videoid;

        public VideoVertexId(){}

        public String getVideoId() {
            return videoid;
        }
    }
}
