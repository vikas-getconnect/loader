package com.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.transactions.Transactions;
import com.couchbase.transactions.TransactionsReactive;
import com.couchbase.transactions.config.TransactionConfig;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.cli.*;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;


import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class loader {
    private static final String CHAR_LIST =
            "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final int NTHREDS = 1000;

    public static void main(String[] args) throws InterruptedException {
        //readArgs(args);

        SimpleTransaction transaction=new SimpleTransaction();
        TransactionConfig config = transaction.createTransactionConfig(100, 0);
        Cluster cluster=Cluster.connect(ClusterEnvironment.create("10.143.190.101","Administrator","password"));
        Bucket bucket=cluster.bucket("default");
        Collection b = bucket.defaultCollection();
        List<Collection> c=new ArrayList<>();
        c.add(b);
        //Transactions t = transaction.createTansaction(cluster, config);
        TransactionsReactive t= transaction.createTansaction(cluster,config).reactive();
        System.out.println("transaction created");
//        List<Tuple2<String, Object>> documenets = getDocuments(100);
//        transaction.RunTransaction(t,c,documenets,new ArrayList<>(),new ArrayList<>(),true,true);

//        ExecutorService executor = Executors.newFixedThreadPool(NTHREDS);
//        for (int i = 0; i < 5000; i++) {
//            List<Tuple2<String, Object>> documenets = getDocuments(1);
//            RunTransactions worker = new RunTransactions(t, c, documenets, new ArrayList<>(), new ArrayList<>(), true, true);
//            executor.execute(worker);
//        }
//        executor.shutdown();
//        // Wait until all threads are finish
//        executor.awaitTermination(100,TimeUnit.MINUTES);
//        System.out.println("Finished all threads");

        while(true) {
            SimpleTransaction tran = new SimpleTransaction();
            RateLimiter rateLimiter = RateLimiter.create(1);
            if (rateLimiter.tryAcquire()) {
                rateLimiter.acquire();
                List<Tuple2<String, Object>> documents = getDocuments(1);
                //tran.RunTransaction(t, c, documents, new ArrayList<>(), new ArrayList<>(), true, true);
                System.out.println(new Date() + ": doc created");
                tran.createDocuments(t,b,documents.get(0));
            }
        }
    }

    public static CommandLine readArgs(String[] args){
        Options options=new Options();

        Option host=new Option("h",true,"host address");
        host.setRequired(true);
        host.setType(String.class);
        Option username=new Option("u",true,"username");
        //username.setRequired(true);
        username.setType(String.class);
        Option password=new Option("p",true,"password");
        //password.setRequired(true);
        password.setType(String.class);

        options.addOption(host);
        options.addOption(username);
        options.addOption(password);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd=null;
        try {
            cmd = parser.parse( options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("loader", options);

            System.exit(1);
        }
        System.out.println(cmd.getArgList());
        System.out.println(cmd.getOptionValue("h"));

        return cmd;
    }

    public static List<String> generateKeys(Integer number){
        List<String> keys=new ArrayList<>();
        for (int i = 0; i < number; i++) {
            keys.add(generateRandomStringUsingSecureRandom(10));
        }
        return keys;
    }

    public static List<Tuple2<String, Object>> getDocuments(Integer number){
        List<String> keys=generateKeys(number);
        JsonObject json = JsonObject.create();
        json.put("vikas","test");
        List<Tuple2<String, Object>> doc=new ArrayList<>();
        for (String key:keys) {
            Tuple2<String, Object> jdoc=Tuples.of(key,json);
            doc.add(jdoc);
        };
        return doc;
    }

    public static String generateRandomStringUsingSecureRandom(int length) {
        StringBuffer randStr = new StringBuffer(length);
        SecureRandom secureRandom = new SecureRandom();
        for (int i = 0; i < length; i++)
            randStr.append(CHAR_LIST.charAt(secureRandom.nextInt(CHAR_LIST.length())));
        return randStr.toString();
    }

}
