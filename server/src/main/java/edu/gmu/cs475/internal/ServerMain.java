package edu.gmu.cs475.internal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.stream.Collectors;

import edu.gmu.cs475.FileManagerServer;
import edu.gmu.cs475.IFileManagerServer;

public class ServerMain {

	public static Path BASEDIR = Paths.get(System.getProperty("user.dir").replace("client","server"), "testdir");;
	public static void main(String[] args) throws IOException {
		if(args.length != 1)
		{
			System.err.println("Error: expected usage: java -jar server.jar <portnumber>");
			return;
		}
		int port = Integer.valueOf(args[0]);
		FileManagerServer lockServer = new FileManagerServer();
		lockServer.init(Files.walk(BASEDIR).filter(Files::isRegularFile).collect(Collectors.toList()));
		IFileManagerServer stub = (IFileManagerServer) UnicastRemoteObject.exportObject(lockServer, 0);
		Registry registry = LocateRegistry.createRegistry(port);
		registry.rebind(IFileManagerServer.RMI_NAME, stub);
		System.out.println("Server bound to port " + port);
		
	}

}
