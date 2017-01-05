/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package org.apache.activemq.broker.registry.base;

import org.apache.activemq.broker.registry.zookeeper.ZookeeperConfiguration;
import org.apache.activemq.broker.registry.zookeeper.ZookeeperRegistryCenter;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * 用于协调分布式服务的注册中心.
 *
 * @author zhangliang
 */
public abstract class CoordinatorRegistryCenter implements RegistryCenter {

    private static CoordinatorRegistryCenter coordinatorRegistryCenter;

    public static CoordinatorRegistryCenter create() {
        if (coordinatorRegistryCenter == null) {
            synchronized (CoordinatorRegistryCenter.class) {
                if (coordinatorRegistryCenter == null) {
                    String zkServers = System.getProperty("zkServers");
                    if (StringUtils.isEmpty(zkServers)) {
                        throw new IllegalArgumentException("zkServers not specified");
                    }
                    coordinatorRegistryCenter = new ZookeeperRegistryCenter(new ZookeeperConfiguration(zkServers, "jkop-msg/amqx"));
                }
            }
        }
        return coordinatorRegistryCenter;
    }

    /**
     * 直接从注册中心而非本地缓存获取数据.
     *
     * @param key 键
     * @return 值
     */
    public abstract String getDirectly(String key);

    public abstract <T> T getDirectly(String key, Class<T> clazz);

    /**
     * 获取子节点名称集合.
     *
     * @param key 键
     * @return 子节点名称集合
     */
    public abstract List<String> getChildrenKeys(String key);

    /**
     * 获取子节点数量.
     *
     * @param key 键
     * @return 子节点数量
     */
    public abstract int getNumChildren(String key);

    /**
     * 持久化临时注册数据.
     *
     * @param key   键
     * @param value 值
     */
    public abstract void persistEphemeral(String key, String value);

    /**
     * 持久化顺序注册数据.
     *
     * @param key 键
     * @return 包含10位顺序数字的znode名称
     */
    public abstract String persistSequential(String key, String value);

    /**
     * 持久化临时顺序注册数据.
     *
     * @param key 键
     */
    public abstract void persistEphemeralSequential(String key);

    /**
     * 添加本地缓存.
     *
     * @param cachePath 需加入缓存的路径
     */
    public abstract void addCacheData(String cachePath);

    public abstract void addCacheData(String cachePath, CacheListenerCallback callback);

    /**
     * 获取注册中心数据缓存对象.
     *
     * @param cachePath 缓存的节点路径
     * @return 注册中心数据缓存对象
     */
    public abstract Object getRawCache(String cachePath);

}
