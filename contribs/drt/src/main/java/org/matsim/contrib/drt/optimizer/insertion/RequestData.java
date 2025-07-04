package org.matsim.contrib.drt.optimizer.insertion;


import com.google.common.base.Verify;
import org.matsim.contrib.drt.passenger.AcceptedDrtRequest;
import org.matsim.contrib.drt.passenger.DrtRequest;

import java.util.*;

public class RequestData {
    private final DrtRequest drtRequest;
	private InsertionRecord solution;

    public RequestData(DrtRequest drtRequest) {
        this.drtRequest = drtRequest;
	}

    public DrtRequest getDrtRequest() {
        return drtRequest;
    }

	public InsertionRecord getSolution() {
		return solution;
	}

	public void setSolution(InsertionRecord solution) {
		Verify.verify(this.solution==null);
		this.solution = solution;
	}

	public record InsertionRecord(Optional<InsertionWithDetourData> insertion, Optional<AcceptedDrtRequest> acceptedDrtRequest, String text) {}

}
