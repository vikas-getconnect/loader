package com.couchbase;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.transactions.*;
import com.couchbase.transactions.config.TransactionConfig;
import com.couchbase.transactions.config.TransactionConfigBuilder;
import com.couchbase.transactions.error.TransactionFailed;
import com.couchbase.transactions.log.LogDefer;
import java.io.PrintStream;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

import com.google.common.util.concurrent.RateLimiter;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

public class SimpleTransaction {
    public SimpleTransaction() {
    }

    public Transactions createTansaction(Cluster cluster, TransactionConfig config) {
        return Transactions.create(cluster, config);
    }

    public TransactionConfig createTransactionConfig(int expiryTimeout, int durabilitylevel) {
        TransactionConfigBuilder config = TransactionConfigBuilder.create();
        switch(durabilitylevel) {
            case 0:
                config.durabilityLevel(TransactionDurabilityLevel.NONE);
                break;
            case 1:
                config.durabilityLevel(TransactionDurabilityLevel.MAJORITY);
                break;
            case 2:
                config.durabilityLevel(TransactionDurabilityLevel.MAJORITY_AND_PERSIST_ON_MASTER);
                break;
            case 3:
                config.durabilityLevel(TransactionDurabilityLevel.PERSIST_TO_MAJORITY);
                break;
            default:
                config.durabilityLevel(TransactionDurabilityLevel.NONE);
        }

        return config.expirationTime(Duration.of((long)expiryTimeout, ChronoUnit.SECONDS)).build();
    }

    public ArrayList<LogDefer> RunTransaction(Transactions transaction, List<Collection> collections, List<Tuple2<String, Object>> Createkeys, List<String> Updatekeys, List<String> Deletekeys, Boolean commit, boolean sync) {
        ArrayList<LogDefer> res = null;
        if (sync) {
            try {
                transaction.run((ctx) -> {
                    Iterator var8 = collections.iterator();

                    Iterator var10;
                    while(var8.hasNext()) {
                        Collection bucketx = (Collection)var8.next();
                        var10 = Createkeys.iterator();

                        while(var10.hasNext()) {
                            Tuple2<String, Object> document = (Tuple2)var10.next();
                            ctx.insert(bucketx, (String)document.getT1(), document.getT2());
                        }
                    }

                    var8 = Updatekeys.iterator();

                    TransactionJsonDocument doc1;
                    String key;
                    Collection bucket;
                    while(var8.hasNext()) {
                        key = (String)var8.next();
                        var10 = collections.iterator();

                        while(var10.hasNext()) {
                            bucket = (Collection)var10.next();
                            if (ctx.get(bucket, key).isPresent()) {
                                doc1 = (TransactionJsonDocument)ctx.get(bucket, key).get();
                                JsonObject content = (JsonObject)doc1.contentAs(JsonObject.class);
                                content.put("mutated", 1);
                                ctx.replace(doc1, content);
                            }
                        }
                    }

                    var8 = Deletekeys.iterator();

                    while(var8.hasNext()) {
                        key = (String)var8.next();
                        var10 = collections.iterator();

                        while(var10.hasNext()) {
                            bucket = (Collection)var10.next();
                            if (ctx.get(bucket, key).isPresent()) {
                                doc1 = (TransactionJsonDocument)ctx.get(bucket, key).get();
                                ctx.remove(doc1);
                            }
                        }
                    }

                    if (commit) {
                        ctx.commit();
                    } else {
                        ctx.rollback();
                    }

                    transaction.close();
                });
            } catch (TransactionFailed var10) {
                System.out.println("Transaction failed from runTransaction");
                ArrayList var10000 = var10.result().log().logs();
                PrintStream var10001 = System.err;
                var10000.forEach(var10001::println);
                res = var10.result().log().logs();
            }
        } else {
            System.out.println("IN ELSE PART");
        }

        return res;
    }

    public void createDocuments(TransactionsReactive transactions,Collection collection,Tuple2<String,Object> document){
        Mono<TransactionResult> result = transactions.run((ctx) -> {
            // Inserting a doc:
            return ctx.insert(collection.reactive(), (String)document.getT1(), document.getT2())
                    .then(ctx.commit());

        }).doOnError(err -> {
            if (err instanceof TransactionFailed) {
                for (LogDefer e : ((TransactionFailed) err).result().log().logs()) {
                    // Optionally, log the result to your own logger
                    System.out.println(e);
                }
            }
        });
        result.block();
    }
}
