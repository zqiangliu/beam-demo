package org.apache.beam.demos.common;
import com.google.gson.Gson;
import org.apache.beam.demos.entity.DatasourceField;
import org.apache.beam.demos.entity.Score;
import org.apache.beam.sdk.coders.CustomCoder;

import java.io.*;

/**
 * @Description
 * @Author liuzhiqiang
 * @Date 2019/8/12 14:50
 */
public class DatasouceFieldCoder extends CustomCoder<DatasourceField> {
    @Override
    public void encode(DatasourceField value, OutputStream outStream) throws IOException {
        System.out.println("---encode");
        ObjectOutputStream oos = new ObjectOutputStream(outStream);
        oos.writeObject(value);
    }

    @Override
    public DatasourceField decode(InputStream inStream) throws IOException {
        System.out.println("---decode");
        ObjectInputStream bis = new ObjectInputStream(inStream);
        try {
            return (DatasourceField) bis.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;

    }
}
