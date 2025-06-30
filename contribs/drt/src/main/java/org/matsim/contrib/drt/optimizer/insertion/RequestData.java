package org.matsim.contrib.drt.optimizer.insertion;

import org.matsim.contrib.drt.passenger.AcceptedDrtRequest;
import org.matsim.contrib.drt.passenger.DrtRequest;

import java.util.*;

public class RequestData {
    private final DrtRequest drtRequest;
	private final int maxSolutions;
	private final List<InsertionRecord> solutions = Collections.synchronizedList(new ArrayList<>());

    public RequestData(DrtRequest drtRequest, int maxSolutions) {
        this.drtRequest = drtRequest;
		this.maxSolutions = maxSolutions;
	}

    public DrtRequest getDrtRequest() {
        return drtRequest;
    }

	public boolean addInsertion(InsertionRecord insertionRecord) {
		if (this.solutions.size() < maxSolutions) {
			this.solutions.add(insertionRecord);
			return true;
		}
		return false;
	}

	public Optional<InsertionRecord> getBestInsertion() {
		return solutions.stream()
			.filter(i -> i.acceptedDrtRequest() != null)
			.min(Comparator.comparing(e -> e.insertion.detourTimeInfo.getTotalTimeLoss()));
	}

	public record InsertionRecord(InsertionWithDetourData insertion, AcceptedDrtRequest acceptedDrtRequest, String text) {}

}
