package edu.gmu.cs475;

import edu.gmu.cs475.internal.ServerMain;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.List;

public abstract class AbstractFileManagerServer implements IFileManagerServer {

	/**
	 * Initializes the server with the state of the files it cares about
	 *
	 * @param files list of the files
	 */
	public abstract void init(List<Path> files);

	/**
	 * Registers a replica with the server, returning all of the files that currently exist.
	 *
	 * @param hostname   the hostname of the replica talking to you (passed again at disconnect)
	 * @param portNumber the port number of the replica talking to you (passed again at disconnect)
	 * @param replica    The RMI object to use to signal to the replica
	 * @return A HashMap of all of the files that currently exist, mapping from filepath/name to its contents
	 * @throws IOException in case of an underlying IOExceptino when reading the files
	 */
	public abstract HashMap<String, String> registerReplica(String hostname, int portNumber, IFileReplica replica) throws IOException;

	/**
	 * Registers a replica with the server, returning all of the files that currently exist.
	 * You don't implement this one. You implement the one that gets passed the client RMI object
	 *
	 * @param hostname   The hostname that should be used to contact this replica
	 * @param portNumber The port number to use to contact the replica
	 * @return A HashMap of all of the files that currently exist, mapping from filepath/name to its contents
	 * @throws RemoteException
	 */
	@Override
	public final HashMap<String, String> registerClient(String hostname, int portNumber) throws RemoteException {
		try {
			System.out.println("Looking for replica at " + hostname + " " + portNumber);
			Registry registry = LocateRegistry.getRegistry(hostname, portNumber);
			IFileReplica replica = (IFileReplica) registry.lookup(IFileReplica.RMI_NAME);
			return registerReplica(hostname, portNumber, replica);
		} catch (Exception e) {
			System.err.println("Client exception connecting to lock server: " + e.toString());
			e.printStackTrace();
			throw new RemoteException();
		}
	}

	/**
	 * Helper method for you to use to read files locally on the server
	 *
	 * @param file
	 * @return
	 * @throws RemoteException
	 * @throws IOException
	 */
	public final String readFileLocally(String file) throws IOException {
		return new String(Files.readAllBytes(Paths.get(file)));
	}

	/**
	 * Helper method for you to use to write files locally on the server
	 *
	 * @param file
	 * @param content
	 * @throws RemoteException
	 * @throws IOException
	 */
	public final void writeFileLocally(String file, String content) throws IOException {
		Path path = Paths.get(file);
		if (!path.startsWith(ServerMain.BASEDIR))
			throw new IOException("Can only write to files in " + ServerMain.BASEDIR);
		Files.write(path, content.getBytes());
	}
}
