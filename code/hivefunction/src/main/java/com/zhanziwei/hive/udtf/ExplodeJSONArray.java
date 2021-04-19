package com.zhanziwei.hive.udtf;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.List;

public class ExplodeJSONArray extends GenericUDTF{
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        if(argOIs.getAllStructFieldRefs().size() != 1) throw new UDFArgumentException("ExplodeJSONArray 只需要一个参数");
        if(!"string".equals(argOIs.getAllStructFieldRefs().get(0).getFieldObjectInspector().getTypeName()))
            throw new UDFArgumentException("json_array_to_struct_array的第一个参数为string类型");
        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldOIs = new ArrayList<>();

        fieldNames.add("items");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);

    }

    @Override
    public void process(Object[] objects) throws HiveException {
        String jsonArray = objects[0].toString();
        JSONArray actions = new JSONArray(jsonArray);

        for(int i = 0; i < actions.length(); i++) {
            String[] result = new String[1];
            result[0] = actions.getString(i);
            forward(result);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
