package com.alibaba.datax.core;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import org.apache.commons.lang.Validate;

/**
 * 执行容器的抽象类，持有该容器全局的配置 configuration
 */
public abstract class AbstractContainer {
    protected Configuration configuration;

    protected AbstractContainerCommunicator containerCommunicator;

    protected String definedJobId;

    public AbstractContainer(Configuration configuration) {
        Validate.notNull(configuration, "Configuration can not be null.");

        this.configuration = configuration;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public AbstractContainerCommunicator getContainerCommunicator() {
        return containerCommunicator;
    }

    public void setContainerCommunicator(AbstractContainerCommunicator containerCommunicator) {
        this.containerCommunicator = containerCommunicator;
    }

    public String getDefinedJobId() {
        return definedJobId;
    }

    public void setDefinedJobId(String definedJobId) {
        this.definedJobId = definedJobId;
    }

    public abstract void start();

}
