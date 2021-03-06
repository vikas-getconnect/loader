package com.couchbase.loader;

import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;

import com.couchbase.transactions.Transactions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.util.function.Tuple2;

import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

public class RunTransactions implements Runnable {
    private Transactions trans;
    private Collection collections;
    private List<Tuple2<String, JsonObject>> doc;
    private List<String> updateKeys;
    private List<String> deleteKeys;
    private Boolean commit;
    private Boolean sync;
    private TransactionHelper transactionHelper=new TransactionHelper();
    private ReadArgs readArgs;
    private static final Logger log = LogManager.getLogger(RunTransactions.class);

    RunTransactions(Transactions trans, Collection collections, List<Tuple2<String, JsonObject>> doc, ReadArgs param){
            this.trans=trans;
            this.collections=collections;
            this.doc=doc;
            this.readArgs=param;
    }

    @Override
    public void run() {
        log.debug(Thread.currentThread().getName());
        Queue<String> ids = batchInsert(trans, collections, doc,Thread.currentThread().getName());
        //System.out.println("update count:"+readArgs.getUpdateCount());
        if(readArgs.getUpdateCount() > 0) {
            List<String> updatelist = ids.stream().limit(readArgs.getUpdateCount()).collect(Collectors.toList());
            batchUpdate(trans, collections, updatelist);
        }
        //System.out.println("delete count:"+readArgs.getDeleteCount());
        if(readArgs.getDeleteCount() > 0) {
            List<String> deletelist = ids.stream().limit(readArgs.getDeleteCount()).collect(Collectors.toList());
            batchDelete(trans, collections, deletelist);
        }
    }

    public Queue<String> batchInsert(Transactions transaction, Collection collection, List<Tuple2<String, JsonObject>> documents,String thread){
        return transactionHelper.multiInsertSingelTransaction(transaction,collection,documents,thread);
    }

    public void batchUpdate(Transactions transaction,Collection collection,List<String> ids){
        transactionHelper.multiUpdateSingelTransaction(transaction,collection,ids);
    }

    public void batchDelete(Transactions transaction,Collection collection,List<String> ids){
        transactionHelper.multiDeleteSingelTransaction(transaction,collection,ids);
    }
}
