package com.couchbase.loader;

import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.transactions.TransactionDurabilityLevel;
import org.apache.commons.cli.*;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;

public class ReadArgs {
    private String hostname;
    private String username;
    private String password;
    private PersistTo persistTo;
    private ReplicateTo replicateTo;
    private TransactionDurabilityLevel durability;
    private String bucket;
    private String collection;
    private Integer ops;
    private Integer docs;
    private Integer time;
    private String type;
    private Integer threads;
    private Integer createCount;
    private Integer updateCount;
    private Integer deleteCount;

    public Integer getCreateCount() {
        return createCount/2;
    }

    public void setCreateCount(Integer createCount) {
        this.createCount = createCount;
    }

    public Integer getUpdateCount() {
        return updateCount/2;
    }

    public void setUpdateCount(Integer updateCount) {
        this.updateCount = updateCount;
    }

    public Integer getDeleteCount() {
        return deleteCount/2;
    }

    public void setDeleteCount(Integer deleteCount) {
        this.deleteCount = deleteCount;
    }

    public Integer getThreads() {
        return threads;
    }

    public void setThreads(Integer threads) {
        this.threads = threads;
    }

    public String getType() { return type; }

    public void setType(String type) { this.type = type; }

    public Integer getTime() {
        return time;
    }

    public void setTime(Integer time) {
        this.time = time;
    }

    public Integer getOps() {
        return ops;
    }

    public void setOps(Integer ops) {
        this.ops = ops;
    }

    public Integer getDocs() {
        return docs;
    }

    public void setDocs(Integer docs) {
        this.docs = docs;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public PersistTo getPersistTo() {
        return persistTo;
    }

    public void setPersistTo(PersistTo persistTo) {
        this.persistTo = persistTo;
    }

    public ReplicateTo getReplicateTo() {
        return replicateTo;
    }

    public void setReplicateTo(ReplicateTo replicateTo) {
        this.replicateTo = replicateTo;
    }

    public TransactionDurabilityLevel getDurability() {
        return durability;
    }

    public void setDurability(TransactionDurabilityLevel durability) {
        this.durability = durability;
    }

    public CommandLine getArgs(String[] args){
        Options options=new Options();

        Option host=new Option("h",true,"host address");
        host.setRequired(true);
        host.setType(String.class);


        Option username=new Option("user",true,"username");
        username.setType(String.class);


        Option password=new Option("password",true,"password");
        password.setType(String.class);

        Option bucket=new Option("b",true,"bucket name or default");
        bucket.setType(String.class);

        Option replicateTo=new Option("replicateTo",true,
                "[0=NONE, 1=ONE, 2=TWO, 3=THREE, default=NONE]");
        replicateTo.setType(Integer.class);

        Option persistTo=new Option("persistTo",true,
                "[0=NONE, 1=ONE, 2=TWO, 3=THREE, 4=FOUR, default=ACTIVE]");
        persistTo.setType(Integer.class);

        Option durability=new Option("durability",true,
                "[MAJORITY,MAJORITY_AND_PERSIST_ON_MASTER,PERSIST_TO_MAJORITY]");
        durability.setType(String.class);

        Option ops=new Option("o",true, "ops rate");
        ops.setType(Integer.class);

        Option items=new Option("i",true, "items");
        items.setType(Integer.class);

//        Option time=new Option("t",true, "time in sec");
//        time.setType(Integer.class);

        Option type=new Option("type",true, "Parallel transactions or parallel insert (bulk/batch)");
        type.setType(String.class);

        Option threads=new Option("t",true, "Number of threads");
        threads.setType(Integer.class);

        Option create=new Option("c",true, "create %");
        create.setType(Integer.class);
        create.setRequired(true);

        Option update=new Option("u",true, "update %");
        update.setType(Integer.class);

        Option delete=new Option("d",true, "delete %");
        delete.setType(Integer.class);

        options.addOption(host);
        options.addOption(username);
        options.addOption(password);
        //options.addOption(replicateTo);
        //options.addOption(persistTo);
        options.addOption(durability);
        options.addOption(bucket);
        options.addOption(ops);
        options.addOption(items);
        //options.addOption(time);
        options.addOption(type);
        options.addOption(threads);
        options.addOption(create);
        options.addOption(update);
        options.addOption(delete);


        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd=null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("loader", options,true);
            System.exit(1);
        }
        return cmd;
    }

    public CommandLine setArgs(String[] args){
        CommandLine cmd=getArgs(args);
        hostname=cmd.getOptionValue("h");
        if(cmd.getOptionValue("user")==null){
            username="Administrator";
        }
        if(cmd.getOptionValue("password")==null){
            password="password";
        }
        if(cmd.getOptionValue("b")==null){
            bucket="default";
        }
        if(cmd.getOptionValue("t")==null){
            time=172800;
        }
        if(cmd.getOptionValue("persistTo")==null){
            persistTo=getPersistTo(0);
        }
        if(cmd.getOptionValue("persistTo")!=null){
            persistTo=getPersistTo(Integer.parseInt(cmd.getOptionValue("persistTo")));
        }
        if(cmd.getOptionValue("replicateTo")!=null){
            replicateTo=getReplicateTo(Integer.parseInt(cmd.getOptionValue("replicateTo")));
        }
        if(cmd.getOptionValue("type")==null){
            type="bulk";
        }
        durability=getDurabilityLevel(cmd.getOptionValue("durability"));
        type=cmd.getOptionValue("type");
        ops = (cmd.getOptionValue("o")==null) ? 1000 : Integer.parseInt(cmd.getOptionValue("o"));
        threads = (cmd.getOptionValue("t")==null) ? 8 : Integer.parseInt(cmd.getOptionValue("t"));
        createCount = (Integer.parseInt(cmd.getOptionValue("c"))*ops/100)/threads;
        updateCount = (cmd.getOptionValue("u")==null) ? 0 :(Integer.parseInt(cmd.getOptionValue("u"))*ops/100)/threads;
        deleteCount = (cmd.getOptionValue("d")==null) ? 0 :(Integer.parseInt(cmd.getOptionValue("d"))*ops/100)/threads;
        return cmd;
    }

    private PersistTo getPersistTo(int persistTo) {
        switch (persistTo) {
            case 0:
                return PersistTo.NONE;
            case 1:
                return PersistTo.ONE;
            case 2:
                return PersistTo.TWO;
            case 3:
                return PersistTo.THREE;
            case 4:
                return PersistTo.FOUR;
            default:
                return PersistTo.ACTIVE;
        }

    }

    private ReplicateTo getReplicateTo(int replicateTo) {
        switch (replicateTo) {
            case 0:
                return ReplicateTo.NONE;
            case 1:
                return ReplicateTo.ONE;
            case 2:
                return ReplicateTo.TWO;
            case 3:
                return ReplicateTo.THREE;
            default:
                return ReplicateTo.NONE;
        }
    }

    private TransactionDurabilityLevel getDurabilityLevel(String durabilityLevel) {
        if(durabilityLevel==null)
            return TransactionDurabilityLevel.NONE;
        if (durabilityLevel.equalsIgnoreCase("MAJORITY")) {
            return TransactionDurabilityLevel.MAJORITY;
        }
        if (durabilityLevel.equalsIgnoreCase("MAJORITY_AND_PERSIST_ON_MASTER")) {
            return TransactionDurabilityLevel.MAJORITY_AND_PERSIST_ON_MASTER;
        }
        if (durabilityLevel.equalsIgnoreCase("PERSIST_TO_MAJORITY")) {
            return TransactionDurabilityLevel.PERSIST_TO_MAJORITY;
        }
        else {
            return TransactionDurabilityLevel.NONE;
        }
    }

    private Duration getDuration(long time, String timeUnit) {
        TemporalUnit temporalUnit;
        if (timeUnit.equalsIgnoreCase("seconds")) {
            temporalUnit = ChronoUnit.SECONDS;
        } else if (timeUnit.equalsIgnoreCase("milliseconds")) {
            temporalUnit = ChronoUnit.MILLIS;
        } else if (timeUnit.equalsIgnoreCase("minutes")) {
            temporalUnit = ChronoUnit.MINUTES;
        } else {
            temporalUnit = ChronoUnit.SECONDS;
        }
        return Duration.of(time, temporalUnit);
    }

    public static void main(String[] args) {
        ReadArgs readArgs=new ReadArgs();
        //CommandLine cmd = readArgs.getArgs(args);
        readArgs.setArgs(args);
        System.out.println(readArgs.getPersistTo().name());
    }
}
