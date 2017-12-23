package com.parking;

import java.util.ArrayList;
import java.util.List;

public class ParkingDetails {

	public int slotNo;
	public String registrationNo;
	public String color;
	public static List<Integer> freeSlots = new ArrayList<Integer>();

	public static void SlotCreation(int totalSolt) {
		if (totalSolt > 0) {
			for (int i = 1; i <= totalSolt; i++) {
				freeSlots.add(i);
			}
		} else {
			System.out.println("please provide valid slot no:");
		}
	}

	public ParkingDetails(String registrationNo, String color) {
		// SSystem.out.println(freeSlots);
		if (freeSlots.isEmpty()) {
			System.out.println("Sorry , parking lot is full");
		} else {
			System.out.println("Allocated slot number:" + freeSlots.get(0));
			this.slotNo = freeSlots.get(0);
			this.registrationNo = registrationNo;
			this.color = color;
			freeSlots.remove(new Integer(freeSlots.get(0)));
		}
	}

	public static void freeParkingLot(List<ParkingDetails> park, int slotNo) {

		try {
			park.remove(slotNo);
			System.out.println("Slot number " + (slotNo + 1) + " is free");
			freeSlots.add(slotNo + 1);
		} catch (Exception e) {
			System.out.println("invalid slot");
		}

	}

	public static void showDetails(List<ParkingDetails> parkingLot) {

		System.out.println("Slot No. Registration No Color:");
		for (ParkingDetails park : parkingLot) {

			if (park.registrationNo != null && park.color != null)
				System.out.println(park.slotNo + "\t" + park.registrationNo + "\t" + park.color);
		}
	}

}
