package com.parking;

import java.util.ArrayList;
import java.util.List;

public class Instruction {

	public static String CREATE_PARKING_LOT = "create_parking_lot";
	public static String PARK = "park";
	public static String LEAVE = "leave";
	public static String STATUS = "status";
	public static String REGISTRATION_NUMBERS_FOR_CARS_WITH_COLOUR = "registration_numbers_for_cars_with_colour";
	public static String SLOT_NUMBERS_FOR_CARS_WITH_COLOUR = "slot_numbers_for_cars_with_colour";
	public static String SLOT_NUMBER_FOR_REGISTRATION_NUMBER = "slot_number_for_registration_number";
	public static List<ParkingDetails> parkingLot = new ArrayList<ParkingDetails>();

	public static void provideCommand(String command) {

		if (command.split(" ")[0].equalsIgnoreCase(CREATE_PARKING_LOT) && command.split(" ").length == 2) {
			String noOfParkingLot = command.split(" ")[1];
			if (Integer.parseInt(noOfParkingLot) > 0) {
				ParkingDetails.SlotCreation(Integer.parseInt(noOfParkingLot));

				System.out.println("Created a parking lot with " + noOfParkingLot + " slots.");
			} else
				System.out.println("please provide valid parking slot no");
		} else if (command.split(" ")[0].equalsIgnoreCase(PARK) && command.split(" ").length == 3) {
			String registrationNo = command.split(" ")[1];
			String color = command.split(" ")[2];
			try {
				parkingLot.add(new ParkingDetails(registrationNo, color));
			} catch (Exception e) {
				System.out.println("");
			}
		} else if (command.split(" ")[0].equalsIgnoreCase(LEAVE) && command.split(" ").length == 2) {
			String slotNo = command.split(" ")[1];
			ParkingDetails.freeParkingLot(parkingLot, Integer.parseInt(slotNo) - 1);
		} else if (command.split(" ")[0].equalsIgnoreCase(STATUS) && command.split(" ").length == 1) {
			ParkingDetails.showDetails(parkingLot);
		} else if (command.split(" ")[0].equalsIgnoreCase(REGISTRATION_NUMBERS_FOR_CARS_WITH_COLOUR) && command.split(" ").length == 2) {
			SpecificDetailSearch.registrationNumbersForCarsWithColour(parkingLot, command.split(" ")[1]);
		} else if (command.split(" ")[0].equalsIgnoreCase(SLOT_NUMBERS_FOR_CARS_WITH_COLOUR) && command.split(" ").length == 2) {
			SpecificDetailSearch.slotNumbersForCarsWithColour(parkingLot, command.split(" ")[1]);
		} else if (command.split(" ")[0].equalsIgnoreCase(SLOT_NUMBER_FOR_REGISTRATION_NUMBER) && command.split(" ").length == 2) {
			SpecificDetailSearch.slotNumberForRegistrationNumber(parkingLot, command.split(" ")[1]);
		}
		else
			System.out.println("Invalid input instruction");
	}

}
