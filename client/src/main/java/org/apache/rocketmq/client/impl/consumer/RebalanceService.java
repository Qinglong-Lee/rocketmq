/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.logging.InternalLogger;

public class RebalanceService extends ServiceThread {
    //liqinglong: 循环执行负载均衡的间隔时间
    private static long waitInterval =
        Long.parseLong(System.getProperty(
            "rocketmq.client.rebalance.waitInterval", "20000"));
    private final InternalLogger log = ClientLogger.getLog();
    private final MQClientInstance mqClientFactory;

    public RebalanceService(MQClientInstance mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");
        //liqinglong: 【循环+指定阻塞时间】的方式达到【定时任务】的效果，这里相当于每隔【waitInterval】20s 做一次负载均衡
        while (!this.isStopped()) {
            //liqinglong: 不会马上执行负载均衡，而是先利用【ServiceThread】中的【countDownLatch】阻塞等待【20s】
            //将会在客户端实现类（如 DefaultMQPushConsumerImpl）的 start() 方法最后调用 MQClientInstance.rebalanceImmediately() 第一次唤醒阻塞的线程
            this.waitForRunning(waitInterval);
            //liqinglong: 线程唤醒后执行负载均衡
            //先去【broker】更新【组内所有客户信息】，在根据【分配策略】进行【队列重分配】
            this.mqClientFactory.doRebalance();
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
        return RebalanceService.class.getSimpleName();
    }
}
