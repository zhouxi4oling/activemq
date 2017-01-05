package org.apache.activemq.broker.registry.base;

/**
 * Created by zhouxiaoling on 2016/12/9.
 */
public interface CacheListenerCallback {

    void onNodeChanged() throws Exception;

}
