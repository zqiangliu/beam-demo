package org.apache.beam.demos.common;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;

import java.io.*;

/**
 * @Description
 * @Author liuzhiqiang
 * @Date 2019/8/12 14:50
 */
public  class MyCoder<T> extends CustomCoder<T> {
    @Override
    public void encode(T value, OutputStream outStream) throws CoderException, IOException {
        ObjectOutputStream oos = new ObjectOutputStream(outStream);
        oos.writeObject(value);
    }

    @Override
    public T decode(InputStream inStream) throws CoderException, IOException {
//        System.out.println("---decode");
        ObjectInputStream bis = new ObjectInputStream(inStream);
        try {
            return (T) bis.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }
}
