package com.parking;

import java.util.ArrayList;
import java.util.List;

public class SpecificDetailSearch {

	public static List<String> registrationNumbersForCarsWithColour(List<ParkingDetails> parkingCar,
			String colour) {
		List<String> registrationNumber = new ArrayList<String>();
		for (ParkingDetails p : parkingCar) {

			if (colour.equalsIgnoreCase(p.color)) {

				registrationNumber.add(p.registrationNo);

			}
		}

		show(registrationNumber);
		return registrationNumber;

	}

	public static List<Integer> slotNumbersForCarsWithColour(List<ParkingDetails> parkingCar, String color) {
		List<Integer> slotNo = new ArrayList<Integer>();
		for (ParkingDetails p : parkingCar) {
			if (color.equalsIgnoreCase(p.color)) {
				slotNo.add(p.slotNo);
			}
		}
		show(slotNo);
		return slotNo;
	}

	public static List<Integer> slotNumberForRegistrationNumber(List<ParkingDetails> parkingCar,
			String registrationNo) {
		List<Integer> slotNo = new ArrayList<Integer>();
		for (ParkingDetails p : parkingCar) {
			if (registrationNo.equalsIgnoreCase(p.registrationNo)) {
				slotNo.add(p.slotNo);
			}
		}

		show(slotNo);
		return slotNo;
	}

	public static void show(@SuppressWarnings("rawtypes") List general) {
		if (general.isEmpty()) {
			System.out.println("Not Found");
		} else {
			int size = general.size();
			for (Object detail : general) {

				if (size > 1) {
					System.out.print(detail.toString() + ",");
					size--;
				} else {
					System.out.println(detail.toString());
					size--;
				}
			}
		}
	}

}
