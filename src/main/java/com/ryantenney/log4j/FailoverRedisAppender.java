package com.ryantenney.log4j;

import java.util.Random;

import org.apache.log4j.helpers.LogLog;

/**
 * FailoverRedisAppender uses a shuffled list of host:port pairs from which it uses the first available. 
 * 
 * @author emicklei
 */
public class FailoverRedisAppender extends RedisAppender {

    // Settings
    private String endpoints = null; // comma separated host:port pairs
    private int maxRetries = 10;
    private int secondsBetweenRetry = 10;
    
    public void setSecondsBetweenRetry(int secondsBetweenRetryAll) {
        this.secondsBetweenRetry = secondsBetweenRetryAll;
    }

    public static class HostPort {
        String host;
        int port;

        public HostPort(String host, int port) {
            this.host = host;
            this.port = port;
        }
    }

    private HostPort[] shuffled;
    private int shuffleIndex = 0;
    private int retries = 0;

    @Override
    public void activateOptions() {
        try {
            if (endpoints == null || endpoints.length() == 0)
                throw new IllegalStateException("Must set 'endpoints'");
            String[] endpointList = this.endpoints.split(",");
            // create HostPort list
            this.shuffled = new HostPort[endpointList.length];
            for (int h = 0; h < endpointList.length; h++) {
                String[] hostport = endpointList[h].split(":");
                this.shuffled[h] = new HostPort(hostport[0], Integer.valueOf(hostport[1]));
            }
            this.shuffle(shuffled);
            super.activateOptions();
        } catch (Exception e) {
            LogLog.error("Error during activateOptions", e);
        }
    }
    
    @Override
    protected void handleWriteException(Exception ex) {
        this.safeDisconnect();
        // immediate reconnect using existing host:port if that fails then try all before enterting the retry loop
        while (!this.connect() && this.retries < this.maxRetries) {     
            // here we tried all available host:ports
            retries++;                       
            // now wait and retry the whole list
            try {
                LogLog.debug("Wait before retry all hosts, attempt:"+retries);
                Thread.sleep(this.secondsBetweenRetry * 1000);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        if (this.retries == this.maxRetries) {
            LogLog.debug("Giving up after attempt:"+maxRetries);
        }
    }
    
    /**
     * Fisher–Yates shuffle
     * @param hplist
     */
    private void shuffle(HostPort[] hplist) {
        Random rnd = new Random();
        for (int i = hplist.length - 1; i > 0; i--) {
            int index = rnd.nextInt(i + 1);
            // Simple swap
            HostPort a = hplist[index];
            hplist[index] = hplist[i];
            hplist[i] = a;
        }
    }

    @Override
    protected void createJedis() {
        HostPort hp = this.shuffled[this.shuffleIndex];
        this.setHost(hp.host);
        this.setPort(hp.port);
        super.createJedis();
    }
    
    @Override
    protected synchronized boolean connect() {
        int first = this.shuffleIndex;
        while (true) {            
            if (super.connect()) {
                return true;
            }
            LogLog.debug("Connect failed, trying the next");
            // rotate for the next
            this.shuffleIndex = (this.shuffleIndex + 1) % this.shuffled.length;
                        
            // i tried them all
            if (first == this.shuffleIndex) {
                LogLog.debug("Connect failed, no more hosts to try");
                return false;
            } else {
                this.createJedis();    
            }
        }
    }
    
    public String getEndpoints() {
        return endpoints;
    }

    public void setEndpoints(String endpoints) {
        this.endpoints = endpoints;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }    
}
