package com.meta.utils;

import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.runtime.RuntimeSchema;
import com.google.common.collect.Maps;
import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;


/**
 * @author weitaoliang
 * @version V1.0
 **/
//scalastyle:off
public class ProtoStuffUtils {
    private final static Logger logger = LoggerFactory.getLogger(ProtoStuffUtils.class);
    //需要使用包装类进行序列化和反序列化的class集合
    private static final Set<Class<?>> WRAPPER_SET = new HashSet<>();

    //序列化反序列化包装类Class对象
    private static final Class<SerializeDeserializeWrapper> WRAPPER_CLASS = SerializeDeserializeWrapper.class;

    //序列化、反序列化包装类schema对象
    private static final Schema<SerializeDeserializeWrapper> WRAPPER_SCHEMA = RuntimeSchema.createFrom(WRAPPER_CLASS);

    //缓存对象及schema信息集合
    private static final Map<Class<?>, Schema<?>> CACHE_SCHEMA = Maps.newConcurrentMap();

    //预定义一部分protostuff无法直接序列化、反序列化的对象

    static {
        WRAPPER_SET.add(List.class);
        WRAPPER_SET.add(ArrayList.class);
        WRAPPER_SET.add(CopyOnWriteArrayList.class);
        WRAPPER_SET.add(LinkedList.class);
        WRAPPER_SET.add(Stack.class);

        WRAPPER_SET.add(Map.class);
        WRAPPER_SET.add(HashMap.class);
        WRAPPER_SET.add(TreeMap.class);
        WRAPPER_SET.add(Hashtable.class);
        WRAPPER_SET.add(SortedMap.class);

        WRAPPER_SET.add(Object.class);

        WRAPPER_SET.add(int[].class);
        WRAPPER_SET.add(float[].class);
        WRAPPER_SET.add(long[].class);
        WRAPPER_SET.add(double[].class);
        WRAPPER_SET.add(String[].class);
    }

    public static void registerWrapperClass(Class clazz) {
        WRAPPER_SET.add(clazz);
    }

    //获取序列化对象的schema

    private static <T> Schema<T> getSchema(Class<T> clazz) {
        Schema<T> schema = (Schema<T>) CACHE_SCHEMA.get(clazz);
        if (schema == null) {
            schema = RuntimeSchema.createFrom(clazz);
        }
        return schema;
    }

    public static <T> byte[] serialize(T obj) {
        Class<T> clazz = (Class<T>) obj.getClass();
        LinkedBuffer buffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
        try {
            Object serializeObject = obj;
            Schema schema = WRAPPER_SCHEMA;
            if (!WRAPPER_SET.contains(clazz)) {
                schema = getSchema(clazz);
            } else {
                serializeObject = SerializeDeserializeWrapper.builder(obj);
            }
            return ProtostuffIOUtil.toByteArray(serializeObject, schema, buffer);
        } catch (Exception e) {
            logger.error("反序列化对象异常[" + obj + "]", e);
            throw new IllegalStateException(e.getMessage(), e);
        } finally {
            buffer.clear();
        }
    }

    public static <T> T deserialize(byte[] data, Class<T> clazz) {
        try {
            if (!WRAPPER_SET.contains(clazz)) {
                T message = clazz.newInstance();
                Schema<T> schema = getSchema(clazz);
                ProtostuffIOUtil.mergeFrom(data, message, schema);
                return message;
            } else {
                SerializeDeserializeWrapper<T> wrapper = new SerializeDeserializeWrapper<>();
                ProtostuffIOUtil.mergeFrom(data, wrapper, WRAPPER_SCHEMA);
                return wrapper.getData();
            }

        } catch (Exception e) {
            logger.error("反序列化对象异常["+"]",e);
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public static<T> byte[] serializeList(List<T> objList){
        if(objList==null || objList.isEmpty()){
            throw new RuntimeException("序列化对象列表异常("+objList+")参数异常!");
        }
        Schema<T> schema=(Schema<T>)RuntimeSchema.getSchema(objList.get(0).getClass());
        LinkedBuffer buffer=LinkedBuffer.allocate(1024*1024);
        byte[] bytes=null;
        try{
            ByteArrayOutputStream bos=new ByteArrayOutputStream();
            ProtostuffIOUtil.writeListTo(bos,objList,schema,buffer);
            bytes=bos.toByteArray();
        } catch (Exception e){
            throw new RuntimeException("序列化对象列表异常("+objList+")异常!");
        }
        finally {
            buffer.clear();
        }
        return bytes;
    }
    public static<T> List<T> deserializeList(byte[] bytes,Class<T> targetClass){
        if(bytes==null || bytes.length==0){
            throw new RuntimeException("序列化对象列表异常bytes序列为空");
        }
        Schema<T> schema=RuntimeSchema.getSchema(targetClass);
        List<T> result;
        try{
            result=ProtostuffIOUtil.parseListFrom(new ByteArrayInputStream(bytes),schema);
        }catch (Exception e){
            throw new RuntimeException("序列化对象列表异常");
        }
        return result;
    }

    public  static class SerializeDeserializeWrapper<T> {

        private T data;

        public static <T> SerializeDeserializeWrapper<T> builder(T data) {
            SerializeDeserializeWrapper<T> wrapper = new SerializeDeserializeWrapper<>();
            wrapper.setData(data);
            return wrapper;
        }

        public T getData() {
            return data;
        }

        public void setData(T data) {
            this.data = data;
        }
    }

}
