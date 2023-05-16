package com.open.openmq.client;

import com.open.openmq.common.protocol.NamespaceUtil;
import com.open.openmq.common.utils.NameServerAddressUtils;

/**
 * @Description TODO
 * @Date 2023/2/1 22:54
 * @Author jack wu
 */
public class ClientConfig {
    protected String namespace;
    private String namesrvAddr = NameServerAddressUtils.getNameServerAddresses();

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String withNamespace(String resource) {
        return NamespaceUtil.wrapNamespace(this.getNamespace(), resource);
    }
}
