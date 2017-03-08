package FullTextSearch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.spark.util.CollectionsUtils;

import java.util.*;

/**
 * Created by cuiguangfan on 16-9-28.
 */
@Description(name = "tfidf",
        value = "_FUNC_(string * text,string query) - Return a term frequency in <string, float>")
public final class HiveTFIDF extends UDAF {

    public static class Evaluator implements UDAFEvaluator {
        private static final Log logger = LogFactory.getLog(HiveTFIDF.class);
        private static final String DELIM = " .,?!:;()<>[]\b\t\n\f\r\"\'\\";
        private String[] stopwords = new String[] {"i", "me", "my", "myself", "we", "our", "ours", "ourselves",
                "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself",
                "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their",
                "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these",
                "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has",
                "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but",
                "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with",
                "about", "against", "between", "into", "through", "during", "before", "after",
                "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over",
                "under", "again", "further", "then", "once", "here", "there", "when", "where",
                "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some",
                "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very",
                "s", "t", "can", "will", "just", "don", "should", "now"};
        private Set<String> stopwordSet=new HashSet<String>(Arrays.asList(stopwords));
        public static class PartialResult {
            public Map<Integer,Map<String,Float>> term_frequency_map;// <docid,<word,frequence>>
            public Map<Integer,Integer> documentWordCount_map;//<docid,word_count>
            public Map<String,Set<Integer>> document_frequency_map;// <word,doc_nums>
            String query;
            PartialResult(){
                this.term_frequency_map=new HashMap<Integer,Map<String,Float>>();
                this.documentWordCount_map=new HashMap<Integer,Integer>();
                this.document_frequency_map=new HashMap<String,Set<Integer>>();
            }
        }

        public PartialResult partial;

        @Override
        public void init() {
            this.partial = null;
        }

        public boolean iterate(int docID,String[] array) {
            logger.info("start iterate process");
            // docID is primary id(doc_id),array[array.length-1] is query string,array[0...array.length-2] is columns need query
            if (array == null) {
                return true;
            }
            if (partial == null) {
                this.partial = new PartialResult();
                this.partial.query=array[array.length-1];
            }
            boolean toLowerCase=true;
            StringBuilder sb=new StringBuilder();
            for(int i=0;i<array.length-1;i++){
                sb.append(array[i]);
            }
            logger.info(docID+"-"+sb.toString()+"-"+array[array.length-1]);
            if(!partial.term_frequency_map.containsKey(docID)) partial.term_frequency_map.put(docID,new HashMap<String,Float>());
            final StringTokenizer tokenizer = new StringTokenizer(sb.toString(), DELIM);
            List<String> tokensWithoutStopWords=new ArrayList<String>();
            while (tokenizer.hasMoreElements()) {
                String word = tokenizer.nextToken();
                if (toLowerCase) {
                    word = word.toLowerCase();
                }
                if(stopwordSet.contains(word)) continue;
                tokensWithoutStopWords.add(word);
            }
            partial.documentWordCount_map.put(docID,tokensWithoutStopWords.size());
            // only iterate all can we know word_freqency/doc_word_num
            // *System.out.println(docID+":"+tokenizer.countTokens());
            for(String word:tokensWithoutStopWords) {
                if (toLowerCase) {
                    word = word.toLowerCase();
                }
                // next , update  PartialResult
                // data are like this: doc_id,word
                // first,update term_frequency
                if(!partial.term_frequency_map.get(docID).containsKey(word)) partial.term_frequency_map.get(docID).put(word,0.0f);
                partial.term_frequency_map.get(docID).put(word,partial.term_frequency_map.get(docID).get(word)+1);
                // second,update document_frequency
                if(!partial.document_frequency_map.containsKey(word)) partial.document_frequency_map.put(word,new HashSet<Integer>());
                partial.document_frequency_map.get(word).add(docID);

            }
            return true;
        }

        public PartialResult terminatePartial() {
            logger.info("start terminatePartial process(only run when jobs are split and run on dirrerent data nodes)");
            return partial;
        }

        public boolean merge(PartialResult other) {
            logger.info("start merge process");
            if (other == null) {
                return true;
            }
            if (partial == null) {
                this.partial = new PartialResult();
            }

            // first,union term_frequency(in fact,considering the performance,there is no need union this.partial  and other because thers is nothing need to union)

//            for(Map.Entry<String,Map<String,Integer>> entry:other.term_frequency_map.entrySet()){
//                if(!partial.term_frequency_map.containsKey(entry.getKey())) partial.term_frequency_map.put(entry.getKey(),entry.getValue());
//                else{
//                    Map<String,Integer> otherMap=entry.getValue();
//                    for(Map.Entry<String,Integer> innerEntry:otherMap.entrySet()){
//                        if(!partial.term_frequency_map.get(entry.getKey()).containsKey(innerEntry.getKey()))
//                            partial.term_frequency_map.get(entry.getKey()).put(innerEntry.getKey(),0);
//                        partial.term_frequency_map.get(entry.getKey()).put(innerEntry.getKey(),partial.term_frequency_map.get(entry.getKey()).get(innerEntry.getKey())+innerEntry.getValue());
//                    }
//
//                }
//            }
            // there is no duplicate doc_id(key) when union ParitialResult from two map process,so we just need putall
            partial.term_frequency_map.putAll(other.term_frequency_map);
            // second,union document_frequency,compared to union of term_frequency,there is duplicate word(key)
            for(Map.Entry<String,Set<Integer>> entry:other.document_frequency_map.entrySet()){
                if(!partial.document_frequency_map.containsKey(entry.getKey())) partial.document_frequency_map.put(entry.getKey(),new HashSet<Integer>());
                partial.document_frequency_map.get(entry.getKey()).addAll(entry.getValue());
            }
            return true;
        }

        public Map<IntWritable, FloatWritable> terminate() {
            logger.info("start terminate process");
            if (partial == null) {
                return null;
            }
            // where terminate partial,we now know all word_freqency/doc_word_num,update frequency to tf
            for(Integer docID:partial.term_frequency_map.keySet()){
                Map<String,Float> word_fre=partial.term_frequency_map.get(docID);
                for(String word:word_fre.keySet()){
                    // *System.out.println(word+"-("+word_fre.get(word)+":"+partial.documentWordCount_map.get(docID)+")-"+word_fre.get(word)/partial.documentWordCount_map.get(docID));
                    word_fre.put(word,word_fre.get(word)/partial.documentWordCount_map.get(docID));
                }
            }
            // calculate tf-idf
            List<Map.Entry<Integer,Float>> list=new ArrayList<Map.Entry<Integer,Float>>();
            int doc_nums=partial.term_frequency_map.size();
            for(Map.Entry<Integer,Map<String,Float>> entry:partial.term_frequency_map.entrySet()){
                Map<String,Float> inner=entry.getValue();
                if(inner.containsKey(partial.query)){
                    float tf=inner.get(partial.query);
                    float idf=partial.document_frequency_map.containsKey(partial.query)?partial.document_frequency_map.get(partial.query).size():0;
                    // *System.out.println(tf);
                    // *System.out.println(Math.log10(doc_nums/Math.max(1,idf))+1.0);
                    float tf_idf= (float) (tf*(Math.log10(doc_nums/Math.max(1,idf))+1.0));
                    list.add(new AbstractMap.SimpleEntry<Integer,Float>(entry.getKey(),tf_idf));
                }
            }
            Collections.sort(list, new Comparator<Map.Entry<Integer, Float>>() {
                @Override
                public int compare(Map.Entry<Integer, Float> o1, Map.Entry<Integer, Float> o2) {
                    return -o1.getValue().compareTo(o2.getValue());
                }
            });
            Map<IntWritable,FloatWritable> result=new LinkedHashMap<IntWritable,FloatWritable>();
            for(Map.Entry<Integer,Float> entry:list){
                result.put(new IntWritable(entry.getKey()),new FloatWritable(entry.getValue()));
            }
            this.partial = null;
            return result;
        }
    }

}
