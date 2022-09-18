package com.jobrunr.scheduler.storage;

import com.couchbase.client.core.env.LoggerConfig;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.tesco.scheduler.model.JobMetadata;
import com.tesco.scheduler.model.JobModel;
import lombok.extern.slf4j.Slf4j;
import org.jobrunr.jobs.*;
import org.jobrunr.jobs.mappers.JobMapper;
import org.jobrunr.jobs.states.ScheduledState;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.storage.JobStats;
import org.jobrunr.storage.*;
import org.jobrunr.storage.nosql.NoSqlStorageProvider;
import org.jobrunr.utils.resilience.RateLimiter;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.SpelEvaluationException;
import org.springframework.expression.spel.SpelParseException;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;
import static org.jobrunr.jobs.states.StateName.SCHEDULED;
import static org.jobrunr.storage.JobRunrMetadata.toId;
import static org.jobrunr.storage.StorageProviderUtils.*;
import static org.jobrunr.storage.StorageProviderUtils.BackgroundJobServers.*;
import static org.jobrunr.utils.JobUtils.getJobSignature;
import static org.jobrunr.utils.resilience.RateLimiter.Builder.rateLimit;
import static org.jobrunr.utils.resilience.RateLimiter.SECOND;

@Slf4j
public class CouchbaseDBStorageProvider extends AbstractStorageProvider implements NoSqlStorageProvider {
    private final String bucketName;
    private final Cluster cluster;
    private final Collection jobCollection;
    private final Collection recurringJobCollection;
    private final Collection backgroundJobServerCollection;
    private final Collection metadataCollection;
    private JobMapper jobMapper;

    public CouchbaseDBStorageProvider(String hostName, String username, String password, String bucketName, int connectTimeoutSeconds,
                                      int kvTimeoutSeconds, int queryTimeoutSeconds) {
        this(Cluster.connect(hostName, ClusterOptions.clusterOptions(username, password)
                .environment(ClusterEnvironment.builder()
                    .loggerConfig(LoggerConfig.consoleLogLevel(Level.OFF))
                    .timeoutConfig(TimeoutConfig
                            .connectTimeout(Duration.ofSeconds(connectTimeoutSeconds))
                            .kvTimeout(Duration.ofSeconds(kvTimeoutSeconds))
                            .queryTimeout(Duration.ofSeconds(queryTimeoutSeconds)))
                            .build())), bucketName);
    }

    public CouchbaseDBStorageProvider(Cluster cluster, String bucketName) {
        this(cluster, bucketName, rateLimit().at1Request().per(SECOND));
    }

    public CouchbaseDBStorageProvider(Cluster cluster, String bucketName, RateLimiter changeListenerNotificationRateLimit) {
        super(changeListenerNotificationRateLimit);

        this.bucketName = bucketName;
        this.cluster = cluster;
        jobCollection = cluster.bucket(bucketName).defaultScope().collection(Jobs.NAME);
        recurringJobCollection = cluster.bucket(bucketName).defaultScope().collection(RecurringJobs.NAME);
        backgroundJobServerCollection = cluster.bucket(bucketName).defaultScope().collection(BackgroundJobServers.NAME);
        metadataCollection = cluster.bucket(bucketName).defaultScope().collection(Metadata.NAME);

    }

    //Constructor for unit testing
    public CouchbaseDBStorageProvider(Cluster cluster, String bucketName, Collection jobCollection, Collection recurringJobCollection,
                                      Collection backgroundJobServerCollection, Collection metadataCollection) {
        super(rateLimit().at1Request().per(SECOND));
        this.bucketName = bucketName;
        this.cluster = cluster;
        this.jobCollection = jobCollection;
        this.recurringJobCollection = recurringJobCollection;
        this.backgroundJobServerCollection = backgroundJobServerCollection;
        this.metadataCollection = metadataCollection;

    }

    @Override
    public void setJobMapper(JobMapper jobMapper) {
        this.jobMapper = jobMapper;
    }

    @Override
    public void setUpStorageProvider(DatabaseOptions databaseOptions) {
        // Storage setup will be done manually in Couchbase
    }

    @Override
    public void announceBackgroundJobServer(BackgroundJobServerStatus serverStatus) {
        try {
            JsonObject obj = JsonObject.create()
                    .put("id", serverStatus.getId().toString())
                    .put(FIELD_WORKER_POOL_SIZE, serverStatus.getWorkerPoolSize())
                    .put(FIELD_POLL_INTERVAL_IN_SECONDS, serverStatus.getPollIntervalInSeconds())
                    .put(FIELD_DELETE_SUCCEEDED_JOBS_AFTER, serverStatus.getDeleteSucceededJobsAfter().toString())
                    .put(FIELD_DELETE_DELETED_JOBS_AFTER, serverStatus.getPermanentlyDeleteDeletedJobsAfter().toString())
                    .put(FIELD_FIRST_HEARTBEAT, serverStatus.getFirstHeartbeat().toEpochMilli())
                    .put(FIELD_LAST_HEARTBEAT, serverStatus.getLastHeartbeat().toEpochMilli())
                    .put(FIELD_IS_RUNNING, serverStatus.isRunning())
                    .put(FIELD_SYSTEM_TOTAL_MEMORY, serverStatus.getSystemTotalMemory())
                    .put(FIELD_SYSTEM_FREE_MEMORY, serverStatus.getSystemFreeMemory())
                    .put(FIELD_SYSTEM_CPU_LOAD, serverStatus.getSystemCpuLoad())
                    .put(FIELD_PROCESS_MAX_MEMORY, serverStatus.getProcessMaxMemory())
                    .put(FIELD_PROCESS_FREE_MEMORY, serverStatus.getProcessFreeMemory())
                    .put(FIELD_PROCESS_ALLOCATED_MEMORY, serverStatus.getProcessAllocatedMemory())
                    .put(FIELD_PROCESS_CPU_LOAD, serverStatus.getProcessCpuLoad());
            this.backgroundJobServerCollection.insert(serverStatus.getId().toString(), obj);
            log.info("Job Server created!");
        }
        catch(Exception e) {
            throw new StorageException("Unable to announce BackgroundJobServer.");
        }
    }

    @Override
    public boolean signalBackgroundJobServerAlive(BackgroundJobServerStatus serverStatus) {
        JsonObject obj = JsonObject.create()
                .put("id", serverStatus.getId().toString())
                .put(FIELD_WORKER_POOL_SIZE, serverStatus.getWorkerPoolSize())
                .put(FIELD_POLL_INTERVAL_IN_SECONDS, serverStatus.getPollIntervalInSeconds())
                .put(FIELD_DELETE_SUCCEEDED_JOBS_AFTER, serverStatus.getDeleteSucceededJobsAfter().toString())
                .put(FIELD_DELETE_DELETED_JOBS_AFTER, serverStatus.getPermanentlyDeleteDeletedJobsAfter().toString())
                .put(FIELD_FIRST_HEARTBEAT, serverStatus.getFirstHeartbeat().toEpochMilli())
                .put(FIELD_LAST_HEARTBEAT, serverStatus.getLastHeartbeat().toEpochMilli())
                .put(FIELD_IS_RUNNING, serverStatus.isRunning())
                .put(FIELD_SYSTEM_TOTAL_MEMORY, serverStatus.getSystemTotalMemory())
                .put(FIELD_SYSTEM_FREE_MEMORY, serverStatus.getSystemFreeMemory())
                .put(FIELD_SYSTEM_CPU_LOAD, serverStatus.getSystemCpuLoad())
                .put(FIELD_PROCESS_MAX_MEMORY, serverStatus.getProcessMaxMemory())
                .put(FIELD_PROCESS_FREE_MEMORY, serverStatus.getProcessFreeMemory())
                .put(FIELD_PROCESS_ALLOCATED_MEMORY, serverStatus.getProcessAllocatedMemory())
                .put(FIELD_PROCESS_CPU_LOAD, serverStatus.getProcessCpuLoad());
        if(this.backgroundJobServerCollection.exists(serverStatus.getId().toString()).exists()) {
            this.backgroundJobServerCollection.replace(serverStatus.getId().toString(), obj);
        }
        else {
            throw new ServerTimedOutException(serverStatus, new StorageException("BackgroundJobServer with id " + serverStatus.getId() + " was not found"));
        }
        Object value = this.backgroundJobServerCollection.get(serverStatus.getId().toString()).contentAsObject().get(BackgroundJobServers.FIELD_IS_RUNNING);
        return value != null && (Boolean) value;
    }

    @Override
    public void signalBackgroundJobServerStopped(BackgroundJobServerStatus serverStatus) {
        this.backgroundJobServerCollection.remove(serverStatus.getId().toString());
    }

    @Override
    public List<BackgroundJobServerStatus> getBackgroundJobServers() {
        QueryResult result = cluster.query(StorageConstants.SELECT_QUERY_PREFIX + bucketName + "`._default.background_job_servers ORDER BY firstHeartbeat ASC");
        List<BackgroundJobServerStatus> backgroundJobServerStatusList = new ArrayList<>();
        for(JsonObject obj : result.rowsAsObject()) {
            backgroundJobServerStatusList.add(new BackgroundJobServerStatus(
                    UUID.fromString(obj.getObject(StorageConstants.BACKGROUND_JOB_SERVER_KEY).getString("id")),
                    obj.getObject(StorageConstants.BACKGROUND_JOB_SERVER_KEY).getInt(FIELD_WORKER_POOL_SIZE),
                    obj.getObject(StorageConstants.BACKGROUND_JOB_SERVER_KEY).getInt(FIELD_POLL_INTERVAL_IN_SECONDS),
                    Duration.parse(obj.getObject(StorageConstants.BACKGROUND_JOB_SERVER_KEY).getString(FIELD_DELETE_SUCCEEDED_JOBS_AFTER)),
                    Duration.parse(obj.getObject(StorageConstants.BACKGROUND_JOB_SERVER_KEY).getString(FIELD_DELETE_DELETED_JOBS_AFTER)),
                    (Instant.ofEpochMilli((Long) obj.getObject(StorageConstants.BACKGROUND_JOB_SERVER_KEY).get(FIELD_FIRST_HEARTBEAT))),
                    (Instant.ofEpochMilli((Long) obj.getObject(StorageConstants.BACKGROUND_JOB_SERVER_KEY).get(FIELD_LAST_HEARTBEAT))),
                    obj.getObject(StorageConstants.BACKGROUND_JOB_SERVER_KEY).getBoolean(FIELD_IS_RUNNING),
                    obj.getObject(StorageConstants.BACKGROUND_JOB_SERVER_KEY).getLong(FIELD_SYSTEM_TOTAL_MEMORY),
                    obj.getObject(StorageConstants.BACKGROUND_JOB_SERVER_KEY).getLong(FIELD_SYSTEM_FREE_MEMORY),
                    obj.getObject(StorageConstants.BACKGROUND_JOB_SERVER_KEY).getDouble(FIELD_SYSTEM_CPU_LOAD),
                    obj.getObject(StorageConstants.BACKGROUND_JOB_SERVER_KEY).getLong(FIELD_PROCESS_MAX_MEMORY),
                    obj.getObject(StorageConstants.BACKGROUND_JOB_SERVER_KEY).getLong(FIELD_PROCESS_FREE_MEMORY),
                    obj.getObject(StorageConstants.BACKGROUND_JOB_SERVER_KEY).getLong(FIELD_PROCESS_ALLOCATED_MEMORY),
                    obj.getObject(StorageConstants.BACKGROUND_JOB_SERVER_KEY).getDouble(FIELD_PROCESS_CPU_LOAD)
            ));
        }
        return backgroundJobServerStatusList;
    }

    @Override
    public UUID getLongestRunningBackgroundJobServerId() {
        UUID uuid;
        do {
            uuid = getLongestRunningServerDetails();
        } while (uuid == null);

        return uuid;
    }

    @Override
    public int removeTimedOutBackgroundJobServers(Instant heartbeatOlderThan) {
        QueryResult result = cluster.query(
                StorageConstants.DELETE_QUERY_PREFIX + bucketName + "`._default.background_job_servers WHERE lastHeartbeat < $lastHeartbeat",
                QueryOptions.queryOptions().parameters(JsonObject.create().put("lastHeartbeat", heartbeatOlderThan.toEpochMilli()))
        );

        return result.rowsAsObject().size();
    }

    @Override
    public void saveMetadata(JobRunrMetadata metadata) {
            JsonObject obj = JsonObject.create()
            .put("id", metadata.getId())
            .put(Metadata.FIELD_NAME, metadata.getName())
            .put(Metadata.FIELD_OWNER, metadata.getOwner())
            .put(Metadata.FIELD_VALUE, metadata.getValue())
            .put(Metadata.FIELD_CREATED_AT, metadata.getCreatedAt().toEpochMilli())
            .put(Metadata.FIELD_UPDATED_AT, metadata.getUpdatedAt().toEpochMilli());

            this.metadataCollection.upsert(metadata.getId(), obj);
            notifyMetadataChangeListeners();
    }

    @Override
    public List<JobRunrMetadata> getMetadata(String name) {
        List<JobRunrMetadata> jobRunrMetadata = new ArrayList<>();
        try {
            QueryResult result = cluster.query(
                    StorageConstants.SELECT_QUERY_PREFIX + bucketName + "`._default.metadata WHERE name = $name",
                    QueryOptions.queryOptions().parameters(JsonObject.create().put("name", name)));

            for (JsonObject row : result.rowsAsObject()) {
                jobRunrMetadata.add(new JobRunrMetadata(
                        row.getObject(StorageConstants.METADATA_KEY).getString(Metadata.FIELD_NAME),
                        row.getObject(StorageConstants.METADATA_KEY).getString(Metadata.FIELD_OWNER),
                        row.getObject(StorageConstants.METADATA_KEY).getString(Metadata.FIELD_VALUE),
                        (Instant.ofEpochMilli((Long) row.getObject(StorageConstants.METADATA_KEY).get(Metadata.FIELD_CREATED_AT))),
                        (Instant.ofEpochMilli((Long) row.getObject(StorageConstants.METADATA_KEY).get(Metadata.FIELD_UPDATED_AT)))));
            }
            return jobRunrMetadata;
        } catch (CouchbaseException ex) {
            log.error("Error occurred in getMetadata, details: ", ex);
            return Collections.emptyList();
        }
    }

    @Override
    public JobRunrMetadata getMetadata(String name, String owner) {
        try {
            GetResult result = metadataCollection.get(toId(name, owner));
            JsonObject obj = result.contentAsObject();

            return new JobRunrMetadata(
                    obj.getString(Metadata.FIELD_NAME),
                    obj.getString(Metadata.FIELD_OWNER),
                    obj.getString(Metadata.FIELD_VALUE),
                    Instant.ofEpochMilli((Long) obj.get(Metadata.FIELD_CREATED_AT)),
                    Instant.ofEpochMilli((Long) obj.get(Metadata.FIELD_UPDATED_AT)));
        } catch (DocumentNotFoundException ex) {
            return null;
        }
    }

    @Override
    public void deleteMetadata(String name) {
        QueryResult result = cluster.query(
                StorageConstants.DELETE_QUERY_PREFIX + bucketName + "`._default.metadata WHERE name = $name",
                QueryOptions.queryOptions().parameters(JsonObject.create().put("name", name)));
        int deletedCount = result.rowsAsObject().size();
        notifyMetadataChangeListeners(deletedCount > 0);
    }

    @Override
    public Job save(Job job) {
        try (JobVersioner jobVersioner = new JobVersioner(job)) {
            this.setJobNameAndParseSPEL(job);
            if (jobVersioner.isNewJob()) {
                JsonObject obj = JsonObject.create()
                .put(Jobs.FIELD_ID, job.getId().toString())
                .put(Jobs.FIELD_VERSION, job.getVersion())
                .put(Jobs.FIELD_JOB_AS_JSON, jobMapper.serializeJob(job))
                .put(Jobs.FIELD_JOB_SIGNATURE, job.getJobSignature())
                .put(Jobs.FIELD_STATE, job.getState().name())
                .put(Jobs.FIELD_CREATED_AT, toMicroSeconds(job.getCreatedAt()))
                .put(Jobs.FIELD_UPDATED_AT, toMicroSeconds(job.getUpdatedAt()));
                if (job.hasState(StateName.SCHEDULED)) {
                    obj.put(Jobs.FIELD_SCHEDULED_AT, toMicroSeconds(job.<ScheduledState>getJobState().getScheduledAt()));
                }
                job.getRecurringJobId().ifPresent(recurringJobId -> obj.put(Jobs.FIELD_RECURRING_JOB_ID, recurringJobId));
                jobCollection.insert(job.getId().toString(), obj);
            } else {
                JsonObject updateDocument = JsonObject.create()
                .put(Jobs.FIELD_VERSION, job.getVersion())
                .put(Jobs.FIELD_JOB_AS_JSON, jobMapper.serializeJob(job))
                .put(Jobs.FIELD_STATE, job.getState().name())
                .put(Jobs.FIELD_CREATED_AT, toMicroSeconds(job.getJobState(0).getCreatedAt()))
                .put(Jobs.FIELD_UPDATED_AT, toMicroSeconds(job.getUpdatedAt()));
                if (job.hasState(StateName.SCHEDULED)) {
                    updateDocument.put(Jobs.FIELD_SCHEDULED_AT, toMicroSeconds(((ScheduledState) job.getJobState()).getScheduledAt()));
                }
                job.getRecurringJobId().ifPresent(recurringJobId -> updateDocument.put(Jobs.FIELD_RECURRING_JOB_ID, recurringJobId));

                JsonObject result = jobCollection.get(job.getId().toString()).contentAsObject();

                if(result.size() > 0 && result.getInt(Jobs.FIELD_VERSION) == (job.getVersion() - 1)) {
                    jobCollection.replace(job.getId().toString(), updateDocument);
                }
                else {
                    throw new ConcurrentJobModificationException(job);
                }
            }
            jobVersioner.commitVersion();
        } catch (Exception e) {
            throw new StorageException(e);
        }
        notifyJobStatsOnChangeListeners();
        return job;
    }

    @Override
    public int deletePermanently(UUID id) {
        final int deletedCount = jobCollection.get(id.toString()).contentAsObject().size();
        jobCollection.remove(id.toString());
        notifyJobStatsOnChangeListenersIf(deletedCount > 0);
        return deletedCount;
    }

    @Override
    public Job getJobById(UUID id) {
        JsonObject result = jobCollection.get(id.toString()).contentAsObject();
        if (result.size() > 0) {
            return jobMapper.deserializeJob(result.get(Jobs.FIELD_JOB_AS_JSON).toString());
        }
        throw new JobNotFoundException(id);
    }

    @Override
    public List<Job> save(List<Job> jobs) {
        try (JobListVersioner jobListVersioner = new JobListVersioner(jobs)) {
            for(Job job : jobs) {
                this.setJobNameAndParseSPEL(job);
            }
            if (jobListVersioner.areNewJobs()) {
                final List<JsonObject> jobsToInsert = jobs.stream()
                        .map(this::toInsertDocument)
                        .collect(toList());
                for(JsonObject obj : jobsToInsert) {
                    jobCollection.insert(obj.getString(Jobs.FIELD_ID), obj);
                }
            } else {
                final List<JsonObject> jobsToUpdate = jobs.stream()
                        .map(this::toUpdateDocument)
                        .collect(toList());
                for(JsonObject obj : jobsToUpdate) {
                    jobCollection.replace(obj.getString(Jobs.FIELD_ID), obj);
                }
            }
            jobListVersioner.commitVersions();
        } catch (Exception e) {
            throw new StorageException(e);
        }
        notifyJobStatsOnChangeListenersIf(!jobs.isEmpty());
        return jobs;
    }

    @Override
    public List<Job> getJobs(StateName state, Instant updatedBefore, PageRequest pageRequest) {
        JsonObject paramObject = JsonObject.create();
        paramObject.put(StorageConstants.STATE_KEY, state.name());
        paramObject.put("updatedAt", toMicroSeconds(updatedBefore));
        QueryResult result = cluster.query(StorageConstants.SELECT_QUERY_PREFIX + bucketName + "`._default.jobs WHERE state = $state AND updatedAt < $updatedAt ORDER BY updatedAt ASC",
                QueryOptions.queryOptions().parameters(paramObject));

        List<Job> jobs = new ArrayList<>();
        for (JsonObject obj : result.rowsAsObject()) {
            jobs.add(jobMapper.deserializeJob(obj.getObject("jobs").get(Jobs.FIELD_JOB_AS_JSON).toString()));
        }

        return jobs;
    }

    @Override
    public List<Job> getScheduledJobs(Instant scheduledBefore, PageRequest pageRequest) {
        JsonObject paramObject = JsonObject.create();
        paramObject.put(StorageConstants.STATE_KEY, SCHEDULED.name());
        paramObject.put("scheduledAt", toMicroSeconds(scheduledBefore));
        QueryResult result = cluster.query(StorageConstants.SELECT_QUERY_PREFIX + bucketName + "`._default.jobs WHERE state = $state AND scheduledAt < $scheduledAt",
                QueryOptions.queryOptions().parameters(paramObject));

        List<Job> jobs = new ArrayList<>();
        for (JsonObject obj : result.rowsAsObject()) {
            jobs.add(jobMapper.deserializeJob(obj.getObject("jobs").get(Jobs.FIELD_JOB_AS_JSON).toString()));
        }

        return jobs;
    }

    @Override
    public List<Job> getJobs(StateName state, PageRequest pageRequest) {
        QueryResult result = cluster.query(StorageConstants.SELECT_QUERY_PREFIX + bucketName + "`._default.jobs WHERE state = $state ORDER BY updatedAt ASC",
                QueryOptions.queryOptions().parameters(JsonObject.create().put(StorageConstants.STATE_KEY, state.name())));

        List<Job> jobs = new ArrayList<>();
        for (JsonObject obj : result.rowsAsObject()) {
            jobs.add(jobMapper.deserializeJob(obj.getObject("jobs").get(Jobs.FIELD_JOB_AS_JSON).toString()));
        }

        return jobs;
    }

    @Override
    public Page<Job> getJobPage(StateName state, PageRequest pageRequest) {
        QueryResult result = cluster.query(StorageConstants.SELECT_QUERY_PREFIX + bucketName + "`._default.jobs WHERE state = $state ORDER BY updatedAt DESC",
                QueryOptions.queryOptions().parameters(JsonObject.create().put(StorageConstants.STATE_KEY, state.name())));


        int count = result.rowsAsObject().size();
        if (count > 0) {
            List<Job> jobs = new ArrayList<>();
            for (JsonObject obj : result.rowsAsObject()) {
                jobs.add(jobMapper.deserializeJob(obj.getObject("jobs").get(Jobs.FIELD_JOB_AS_JSON).toString()));
            }
            return new Page<>(count, jobs, pageRequest);
        }
        return new Page<>(0, new ArrayList<>(), pageRequest);
    }

    @Override
    public int deleteJobsPermanently(StateName state, Instant updatedBefore) {
        JsonObject paramObject = JsonObject.create();
        paramObject.put(StorageConstants.STATE_KEY, state.name());
        paramObject.put("createdAt", toMicroSeconds(updatedBefore));
        final QueryResult result = cluster.query(StorageConstants.SELECT_QUERY_PREFIX + bucketName + "`._default.jobs WHERE state = $state AND createdAt < $createdAt",
                QueryOptions.queryOptions().parameters(paramObject));
        cluster.query(StorageConstants.DELETE_QUERY_PREFIX + bucketName + "`._default.jobs WHERE state = $state AND createdAt < $createdAt",
                QueryOptions.queryOptions().parameters(paramObject));

        final int deletedCount = result.rowsAsObject().size();
        notifyJobStatsOnChangeListenersIf(deletedCount > 0);
        return deletedCount;
    }

    @Override
    public Set<String> getDistinctJobSignatures(StateName... states) {
        StringBuffer sb = new StringBuffer();
        sb.append("[");
        stream(states).map(Enum::name).forEach( state -> {
            String stateValue = "'" + state + "'";
            sb.append(stateValue);
        });
        sb.append("]");
        QueryResult result = cluster.query("SELECT DISTINCT jobSignature FROM `" + bucketName + "`._default.jobs WHERE state IN $state",
                QueryOptions.queryOptions().parameters(JsonObject.create().put(StorageConstants.STATE_KEY, sb.toString())));
        Set<String> resultSet = new HashSet<>();
        for(JsonObject obj : result.rowsAsObject()) {
            resultSet.add(obj.getObject("jobs").toString());
        }

        return  resultSet;
    }

    @Override
    public boolean exists(JobDetails jobDetails, StateName... states) {
        StringBuffer sb = new StringBuffer();
        sb.append("[");
        stream(states).map(Enum::name).forEach( state -> {
            String stateValue = "'" + state + "'";
            sb.append(stateValue);
        });
        sb.append("]");
        JsonObject paramObject = JsonObject.create();
        paramObject.put(StorageConstants.STATE_KEY, sb.toString());
        paramObject.put("jobSignature", getJobSignature(jobDetails));
        final QueryResult result = cluster.query(StorageConstants.SELECT_QUERY_PREFIX + bucketName + "`._default.jobs WHERE state IN $state AND jobSignature = $jobSignature",
                QueryOptions.queryOptions().parameters(paramObject));

        return result.rowsAsObject().size() > 0;
    }

    @Override
    public boolean recurringJobExists(String recurringJobId, StateName... states) {
        StringBuffer sb = new StringBuffer();
        sb.append("[");
        stream(states).map(Enum::name).forEach( state -> {
            String stateValue = "'" + state + "'";
            sb.append(stateValue);
        });
        sb.append("]");
        JsonObject paramObject = JsonObject.create();
        paramObject.put(StorageConstants.STATE_KEY, sb.toString());
        paramObject.put("recurringJobId", recurringJobId);
        final QueryResult result = cluster.query(StorageConstants.SELECT_QUERY_PREFIX + bucketName + "`._default.jobs WHERE state IN $state AND recurringJobId = $recurringJobId",
                QueryOptions.queryOptions().parameters(paramObject));

        return result.rowsAsObject().size() > 0;
    }

    @Override
    public RecurringJob saveRecurringJob(RecurringJob recurringJob) {
        if(recurringJob.getJobDetails().getJobParameters().size() > 0 ) {
            String jobName = ((JobModel) recurringJob.getJobDetails().getJobParameters().get(0).getObject()).getJobMetadata().getJobName();
            recurringJob.setJobName(jobName);
        }
        JsonObject obj = JsonObject.create()
        .put(RecurringJobs.FIELD_ID, recurringJob.getId())
        .put(RecurringJobs.FIELD_VERSION, recurringJob.getVersion())
        .put(RecurringJobs.FIELD_JOB_AS_JSON, jobMapper.serializeRecurringJob(recurringJob))
        .put(RecurringJobs.FIELD_CREATED_AT, recurringJob.getCreatedAt().toEpochMilli());

        recurringJobCollection.upsert(recurringJob.getId(), obj);
        return recurringJob;
    }

    @Override
    public RecurringJobsResult getRecurringJobs() {
        final QueryResult result = cluster.query(StorageConstants.SELECT_QUERY_PREFIX + bucketName + StorageConstants.RECURRING_JOBS_QUERY_POSTFIX);
        List<RecurringJob> recurringJobs = new ArrayList<>();
        for (JsonObject obj : result.rowsAsObject()) {
            recurringJobs.add(jobMapper.deserializeRecurringJob(obj.getObject("recurring_jobs").get(RecurringJobs.FIELD_JOB_AS_JSON).toString()));
        }
        return new RecurringJobsResult(recurringJobs);
    }

    @Override
    public boolean recurringJobsUpdated(Long recurringJobsUpdatedHash) {

        final QueryResult result = cluster.query("SELECT s.last_modified_hash FROM (SELECT * FROM `" + bucketName + "`._default.recurring_jobs ORDER BY createdAt ASC) AS s GROUP BY s.last_modified_hash",
                QueryOptions.queryOptions());

        if(result.rowsAsObject().size() > 0 && !result.rowsAsObject().get(0).isEmpty()) {
            Long value = result.rowsAsObject().get(0).getObject("recurring_jobs").getLong(RecurringJobs.FIELD_CREATED_AT);
            return !recurringJobsUpdatedHash.equals(value);
        }
        return !recurringJobsUpdatedHash.equals(0L);
    }

    @Override
    public long countRecurringJobs() {
        QueryResult result = cluster.query(StorageConstants.SELECT_QUERY_PREFIX + bucketName + StorageConstants.RECURRING_JOBS_QUERY_POSTFIX);
        return result.rowsAsObject().size();
    }

    @Override
    public int deleteRecurringJob(String id) {
        try {
            recurringJobCollection.remove(id);
            return 1;
        }
        catch (Exception ex) {
            return 0;
        }
    }

    @Override
    public JobStats getJobStats() {
        Instant instant = Instant.now();
        JsonObject obj;
        Map<String, Long> statsMap = new ConcurrentHashMap<>();
        try {
            obj = metadataCollection.get(Metadata.STATS_ID).contentAsObject();
        } catch (DocumentNotFoundException ex) {
            obj = null;
        }
        final long allTimeSucceededCount = ( obj != null ? ((Number) obj.get(Metadata.FIELD_VALUE)).longValue() : 0L);

        final QueryResult result = cluster.query("SELECT state, COUNT(*) AS count FROM `" + bucketName + "`._default.jobs GROUP BY state", QueryOptions.queryOptions());

        for(JsonObject jsonObject : result.rowsAsObject()) {
            statsMap.put(jsonObject.getString(StorageConstants.STATE_KEY), jsonObject.getLong("count"));
        }

        Long scheduledCount = statsMap.get(StateName.SCHEDULED.toString()) != null ? statsMap.get(StateName.SCHEDULED.toString()) : 0L;
        Long enqueuedCount = statsMap.get(StateName.ENQUEUED.toString()) != null ? statsMap.get(StateName.ENQUEUED.toString()) : 0L;
        Long processingCount = statsMap.get(StateName.PROCESSING.toString()) != null ? statsMap.get(StateName.PROCESSING.toString()) : 0L;
        Long succeededCount = statsMap.get(StateName.SUCCEEDED.toString()) != null ? statsMap.get(StateName.SUCCEEDED.toString()) : 0L;
        Long failedCount = statsMap.get(StateName.FAILED.toString()) != null ? statsMap.get(StateName.FAILED.toString()) : 0L;
        Long deletedCount = statsMap.get(StateName.DELETED.toString()) != null ? statsMap.get(StateName.DELETED.toString()) : 0L;



        final long total = scheduledCount + enqueuedCount + processingCount + succeededCount + failedCount;
        QueryResult result1 = cluster.query(StorageConstants.SELECT_QUERY_PREFIX + bucketName + StorageConstants.RECURRING_JOBS_QUERY_POSTFIX);
        final int recurringJobCount = result1.rowsAsObject().size();
        QueryResult result2 = cluster.query(StorageConstants.SELECT_QUERY_PREFIX + bucketName + "`._default.background_job_servers");
        final int backgroundJobServerCount = result2.rowsAsObject().size();

        return new JobStats(
                instant,
                total,
                scheduledCount,
                enqueuedCount,
                processingCount,
                failedCount,
                succeededCount,
                allTimeSucceededCount,
                deletedCount,
                recurringJobCount,
                backgroundJobServerCount
        );
    }

    @Override
    public void publishTotalAmountOfSucceededJobs(int amount) {
        int initialAmount = 0;
        JsonObject result = null;
        try {
            result = metadataCollection.get(Metadata.STATS_ID).contentAsObject();
        } catch (DocumentNotFoundException ex) {
            log.error(ex.getMessage());
        }
        finally {
            if (result != null && result.size() > 0) {
                initialAmount = result.getInt(Metadata.FIELD_VALUE);
            }
            JsonObject obj = JsonObject.create()
                    .put("id", Metadata.STATS_ID)
                    .put(Metadata.FIELD_VALUE, (initialAmount + amount));
            metadataCollection.upsert(Metadata.STATS_ID, obj);
        }
    }

    private long toMicroSeconds(Instant instant) {
        return ChronoUnit.MICROS.between(Instant.EPOCH, instant);
    }

    private UUID getLongestRunningServerDetails() {
        QueryResult result = cluster.query(StorageConstants.SELECT_QUERY_PREFIX + bucketName + "`._default.background_job_servers ORDER BY firstHeartbeat ASC", QueryOptions.queryOptions());
        if(result.rowsAsObject().size() > 0) {
            return UUID.fromString(result.rowsAsObject().get(0).getObject(StorageConstants.BACKGROUND_JOB_SERVER_KEY).getString("id"));
        }
        return null;
    }

    private JsonObject toInsertDocument(Job job) {
        final JsonObject obj = JsonObject.create()
        .put(Jobs.FIELD_ID, job.getId().toString())
        .put(Jobs.FIELD_VERSION, job.getVersion())
        .put(Jobs.FIELD_JOB_AS_JSON, jobMapper.serializeJob(job))
        .put(Jobs.FIELD_JOB_SIGNATURE, job.getJobSignature())
        .put(Jobs.FIELD_STATE, job.getState().name())
        .put(Jobs.FIELD_CREATED_AT, toMicroSeconds(job.getCreatedAt()))
        .put(Jobs.FIELD_UPDATED_AT, toMicroSeconds(job.getUpdatedAt()));
        if (job.hasState(StateName.SCHEDULED)) {
            obj.put(Jobs.FIELD_SCHEDULED_AT, toMicroSeconds(job.<ScheduledState>getJobState().getScheduledAt()));
        }
        job.getRecurringJobId().ifPresent(recurringJobId -> obj.put(Jobs.FIELD_RECURRING_JOB_ID, recurringJobId));
        return obj;
    }

    private JsonObject toUpdateDocument(Job job) {
        final JsonObject obj = JsonObject.create()
        .put(Jobs.FIELD_ID, job.getId().toString())
        .put(Jobs.FIELD_VERSION, job.getVersion())
        .put(Jobs.FIELD_JOB_AS_JSON, jobMapper.serializeJob(job))
        .put(Jobs.FIELD_STATE, job.getState().name())
        .put(Jobs.FIELD_CREATED_AT, toMicroSeconds(job.getJobState(0).getCreatedAt()))
        .put(Jobs.FIELD_UPDATED_AT, toMicroSeconds(job.getUpdatedAt()));
        if (job.hasState(StateName.SCHEDULED)) {
            obj.put(Jobs.FIELD_SCHEDULED_AT, toMicroSeconds(((ScheduledState) job.getJobState()).getScheduledAt()));
        }
        job.getRecurringJobId().ifPresent(recurringJobId -> obj.put(Jobs.FIELD_RECURRING_JOB_ID, recurringJobId));
        return obj;
    }

    private void setJobNameAndParseSPEL(Job job) {
        if (job.getJobDetails().getJobParameters().size() > 0) {
            String jobName = ((JobModel) job.getJobDetails().getJobParameters().get(0).getObject()).getJobMetadata().getJobName();
            job.setJobName(jobName);
            JobMetadata jobMetadata = ((JobModel) job.getJobDetails().getJobParameters().get(0).getObject()).getJobMetadata();
            Map<String, String> jobAttributes = jobMetadata.getJobAttributes() != null ? jobMetadata.getJobAttributes() : Collections.emptyMap();
            for (Map.Entry<String, String> entry : jobAttributes.entrySet()) {
                if (entry.getValue() != null) {
                    try {
                        ExpressionParser parser = new SpelExpressionParser();
                        Expression exp = parser.parseExpression(entry.getValue());
                        jobAttributes.put(entry.getKey(), String.valueOf(exp.getValue()).equals("0") ? exp.getExpressionString() : String.valueOf(exp.getValue()));
                    } catch (SpelEvaluationException | SpelParseException ignored) {
                    }
                }
            }
        }
    }
}

