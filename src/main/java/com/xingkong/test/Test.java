package com.xingkong.test;

import FullTextSearch.HiveTFIDF.Evaluator;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Arrays;

/**
 * Created by cuiguangfan on 16-10-19.
 */
public class Test {
    public static void main(String[] args){
        Evaluator eva=new Evaluator();
        try {
            BufferedReader br=new BufferedReader(new FileReader("source/test.testformat"));
            String s=null;
            while((s=br.readLine())!=null){
                String[] temp=s.split("\\|");
                int docID=Integer.valueOf(temp[0]);
                String[] array=new String[temp.length];
                for(int i=1;i<temp.length;i++) array[i-1]=temp[i];
                array[temp.length-1]="justice";
                eva.iterate(docID,array);
            }
            System.out.println("document_frequency_map");
            System.out.println(eva.partial.document_frequency_map);
            System.out.println("term_frequency_map");
            System.out.println(eva.partial.term_frequency_map);
            eva.terminatePartial();
            System.out.println(eva.partial.term_frequency_map.get(1).size());
            System.out.println("--------------------------");
            System.out.println("document_frequency_map");
            System.out.println(eva.partial.document_frequency_map);
            System.out.println("term_frequency_map");
            System.out.println(eva.partial.term_frequency_map);
            System.out.println(eva.terminate());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
