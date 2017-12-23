package com.parking;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

public class StartParking {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if (args.length == 1) {
			try {
				File file = new File(args[0]);
				FileReader fileReader = new FileReader(file);
				BufferedReader bufferedReader = new BufferedReader(fileReader);
				String line;
				while ((line = bufferedReader.readLine()) != null) {
					Instruction.provideCommand(line);
				}
				fileReader.close();
			} catch (IOException e) {
				System.out.println("invalid file name or file llocation");
				//e.printStackTrace();
			}

		} else if (args.length == 0) {
			Scanner scan = new Scanner(System.in);
			int num = 1;
			while (num > 0) {
				System.out.println("Input:");
				String command = scan.nextLine();
				System.out.println("Output:");
				Instruction.provideCommand(command);
			}
			;
			scan.close();

		}
		else
		{
			System.out.println("Improper no of argument from coammand line");
		}

	}

}
