package org.matsim.contrib.drt.optimizer.insertion;

import org.matsim.contrib.drt.passenger.DrtRequest;

import java.util.concurrent.CompletableFuture;

public class RequestRecord {
    private DrtRequest drtRequest;
    private CompletableFuture<DrtRequest> cf;

    public RequestRecord(DrtRequest drtRequest, CompletableFuture<DrtRequest> cf) {
        this.drtRequest = drtRequest;
        this.cf = cf;
    }

    public DrtRequest getDrtRequest() {
        return drtRequest;
    }

    public void setDrtRequest(DrtRequest drtRequest) {
        this.drtRequest = drtRequest;
    }

    public CompletableFuture<DrtRequest> getCf() {
        return cf;
    }

    public void setCf(CompletableFuture<DrtRequest> cf) {
        this.cf = cf;
    }

}
