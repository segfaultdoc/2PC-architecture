package edu.gmu.cs475.internal;

import java.io.IOException;

import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStyle;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;

import edu.gmu.cs475.AbstractFileManagerClient;
import edu.gmu.cs475.FileManagerClient;

@ShellComponent
public class Command {

	public static AbstractFileManagerClient service;

	public Command() {
		service = new FileManagerClient("127.0.0.1", Main.port);
	}


	@ShellMethod("List all file names")
	public CharSequence listAllFiles() {
		return service.listAllFiles().toString();
	}

	@ShellMethod("Cat a file")
	public CharSequence cat(String file) {
		try {
			return service.readFile(file);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@ShellMethod("Echo text into a file")
	public CharSequence echo(String file, String text) {
		try {
			service.writeFile(file, text);
		} catch (IOException e) {
			return new AttributedString("Error: " + e.getMessage(), AttributedStyle.DEFAULT.foreground(AttributedStyle.RED));
		}
		return null;
	}

	@ShellMethod("Cat all files")
	public CharSequence catAll() {
		try {
			return service.catAllFiles();
		} catch (IOException e) {
			return new AttributedString("Error: " + e.getMessage(), AttributedStyle.DEFAULT.foreground(AttributedStyle.RED));
		}
	}

	@ShellMethod("Echo text into all files")
	public CharSequence echoAll(String text) {
		try {
			service.echoToAllFiles(text);
			return null;
		} catch (IOException e) {
			return new AttributedString("Error: " + e.getMessage(), AttributedStyle.DEFAULT.foreground(AttributedStyle.RED));
		}
	}
}
