package org.apache.beam.demos.common;

import com.google.gson.Gson;
import com.sun.jdi.PathSearchingVirtualMachine;
import org.apache.beam.demos.entity.Score;
import org.apache.beam.sdk.coders.CustomCoder;

import java.io.*;

/**
 * @Description
 * @Author liuzhiqiang
 * @Date 2019/8/19 9:35
 */
public class ScoreCoder extends CustomCoder<Score> {
    @Override
    public void encode(Score value, OutputStream outStream) throws IOException {
//        Gson gson = new Gson();
//        String jsonStr = gson.toJson(value);
//        System.out.println("- encode " + jsonStr);
//        ByteArrayInputStream bis = new ByteArrayInputStream(jsonStr.getBytes());
//        byte[] buf = new byte[1024];
//        int len;
//        while((len = bis.read(buf)) > 0){
//            outStream.write(buf, 0, len);
//        }
        ObjectOutputStream oos = new ObjectOutputStream(outStream);
        oos.writeObject(value);
    }

    @Override
    public Score decode(InputStream inStream) throws IOException {

        ObjectInputStream bis = new ObjectInputStream(inStream);
        try {
            return (Score)bis.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
//        ByteArrayOutputStream bos = new ByteArrayOutputStream();
//        byte[] buf = new byte[1024];
//        int len;
//        while((len = inStream.read(buf)) > 0){
//            bos.write(buf, 0, len);
//        }
//        String jsonStr = new String(bos.toByteArray(), "utf-8");
//        Gson gson = new Gson();
//        System.out.println("- decode " + jsonStr);
//        return gson.fromJson(jsonStr, Score.class);
    }

    public static void main(String[] args) throws Exception{
        ScoreCoder sc = new ScoreCoder();
        Score s = new Score(1l, 2l, 2f);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        sc.encode(s, bos);
        bos.close();
        byte[] bytes = bos.toByteArray();

        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        sc.decode(is);
        is.close();
    }
}
