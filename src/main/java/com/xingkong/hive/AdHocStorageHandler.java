package com.xingkong.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.derby.iapi.store.raw.log.Logger;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.sql.SparkSession;

import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by cuiguangfan on 16-10-25.
 */
public class AdHocStorageHandler extends DefaultStorageHandler implements HiveStoragePredicateHandler{
    private static final Log logger = LogFactory.getLog(AdHocStorageHandler.class);
    public static String ADHOC_TABLE="adhoc.handler.table.name";
    public static String ADHOC_TABLE_MAPPING="adhoc.handler.mapping";
    public static String ADHOC_TABLE_MAPPING_DYNAMIC="adhoc.handler.dynamic.mapping";
    public static String ADHOC_MASTER="adhoc.handler.master";
    public static String ADHOC_SCHEMA="adhoc.handler.schema";
    public static String[] CONFIG_ALLKEY;
    public static String[] CONFIG_ALLKEY_ENV;
    public static String INTERNAL_TABLENAME;
    public static String TABLE_INIT_TAG;
    static AtomicLong jobIndex;
    static AtomicLong jobId;
    static {
        CONFIG_ALLKEY = new String[]{ADHOC_TABLE, ADHOC_TABLE_MAPPING, ADHOC_MASTER, ADHOC_SCHEMA};
        CONFIG_ALLKEY_ENV = new String[]{"spark.executor.instances", "spark.executor.cores"};
        INTERNAL_TABLENAME = "adhoc_heartbeat";
        INTERNAL_TABLENAME = "adhoc.table.init.tag";
        jobIndex = new AtomicLong(0L);
        jobId = new AtomicLong(0L);
    }
    public static synchronized String getIndex(){
        NumberFormat nf=NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(3);
        nf.setGroupingUsed(false);
        long index=jobIndex.incrementAndGet()%999L;
        return nf.format(index);
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        this.configureTableJobProperties(tableDesc, jobProperties);
    }

    @Override
    public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        this.configureTableJobProperties(tableDesc, jobProperties);
        String sql=(String)SparkSession.builder().getOrCreate().sparkContext().getLocalProperties().get("spark.job.description");
        jobProperties.put("adhoc.output.sql",sql);
        String committer_class=(String)jobProperties.get("mapred.output.committer.class");
        SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMddHHmmss");
        jobProperties.put("adhoc.committer.id","job_"+sdf.format(new Date(System.currentTimeMillis()))+"_"+getIndex());
        if(!YdbOutputCommitter.class.getName().equals(committer_class)) {
            if(committer_class != null) {
                jobProperties.put("mapred.output.committer.class.ya100", committer_class);
            }

            jobProperties.put("mapred.output.committer.class", YdbOutputCommitter.class.getName());
        }
    }
    public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        Properties tableProperties = tableDesc.getProperties();
        Properties systemProperties = System.getProperties();
        String[] allKeyConfig = CONFIG_ALLKEY_ENV;

        for(int i = 0; i < allKeyConfig.length; ++i) {
            String tempKey = allKeyConfig[i];
            if(systemProperties.containsKey(tempKey)) {
                jobProperties.put(tempKey, systemProperties.getProperty(tempKey));
                logger.info("CONFIG_ALLKEY_ENV \t" + tempKey + "\t" + systemProperties.getProperty(tempKey));
            }
        }

        allKeyConfig = CONFIG_ALLKEY;

        for(int i = 0; i < allKeyConfig.length; ++i) {
            String tempKey = allKeyConfig[i];
            if(tableProperties.containsKey(tempKey)) {
                jobProperties.put(tempKey, tableProperties.getProperty(tempKey));
                logger.info("CONFIG_ALLKEY \t" + tempKey + "\t" + tableProperties.getProperty(tempKey));
            }
        }

        jobProperties.put(INTERNAL_TABLENAME, Boolean.TRUE.toString());
    }

    public static synchronized void setJobId(JobConf jobConf) {
        long var1 = jobConf.getLong("adhoc.job.uniq.id", -1L);
        if(var1 < 0L) {
            jobConf.setLong("adhoc.job.uniq.id", jobId.incrementAndGet());
        }

    }

    public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
        try {
            setJobId(jobConf);
            Map var3 = tableDesc.getJobProperties();
            Iterator var4 = var3.entrySet().iterator();

            String var7;
            while(var4.hasNext()) {
                Map.Entry var5 = (Map.Entry)var4.next();
                String var6 = String.valueOf(var5.getKey());
                var7 = String.valueOf(var5.getValue());
                logger.info("configureJobConf getJobProperties\t" + (String)var5.getKey() + "\t" + (String)var5.getValue());
                if(jobConf.get(var6) == null) {
                    jobConf.set(var6, var7);
                }
            }

            Properties var10 = tableDesc.getProperties();
            Iterator var11 = var10.entrySet().iterator();

            String var8;
            while(var11.hasNext()) {
                Map.Entry var13 = (Map.Entry)var11.next();
                var7 = String.valueOf(var13.getKey());
                var8 = String.valueOf(var13.getValue());
                logger.info("configureJobConf map\t" + var13.getKey() + "\t" + var13.getValue());
                if(jobConf.get(var7) == null) {
                    jobConf.set(var7, var8);
                }
            }

            //i.a(jobConf);
            String[] var12 = CONFIG_ALLKEY;
            int var14 = var12.length;

            for(int var15 = 0; var15 < var14; ++var15) {
                var8 = var12[var15];
                if(var10.containsKey(var8)) {
                    jobConf.set(var8, var10.getProperty(var8));
                    logger.info("configureJobConf CONFIG_ALLKEY \t" + var8 + "\t" + var10.getProperty(var8));
                }
            }

            jobConf.setBoolean("hive.mapred.supports.subdirectories", true);
            jobConf.set(TABLE_INIT_TAG, Boolean.TRUE.toString());
        } catch (Exception var9) {
            throw new RuntimeException(var9);
        }
    }

//    public Class<? extends SerDe> getSerDeClass() {
//        return Ya100Serde.class;
//    }
//
//    public Class<? extends InputFormat> getInputFormatClass() {
//        return Ya100HiveTableInputFormat.class;
//    }
//
//    public Class<? extends OutputFormat> getOutputFormatClass() {
//        return Ya100HiveOutputFormat.class;
//    }

    public DecomposedPredicate decomposePredicate(JobConf var1, Deserializer var2, ExprNodeDesc var3) {
        logger.info("decomposePredicate called#######################");
        return null;
    }
}
