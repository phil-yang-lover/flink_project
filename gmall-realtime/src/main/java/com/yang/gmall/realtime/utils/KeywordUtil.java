package com.yang.gmall.realtime.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {
    public static List<String> analyze(String text){
        StringReader stringReader = new StringReader(text);
        IKSegmenter ikSegmenter = new IKSegmenter(stringReader,true);
        Lexeme lexeme = null;
        List list = new ArrayList<>();
        try {
            while ((lexeme = ikSegmenter.next())!=null){
                String lexemeText = lexeme.getLexemeText();
                list.add(lexemeText);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    public static void main(String[] args) {
        List<String> list = analyze("云南白药牙膏");
        System.out.println(list);
    }
}
