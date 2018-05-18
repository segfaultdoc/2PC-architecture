package edu.gmu.cs475;

import edu.gmu.cs475.internal.CaptureMatcher;
import edu.gmu.cs475.internal.NoOpFileManagerClient;
import edu.gmu.cs475.internal.ServerMain;
import org.easymock.Capture;
import org.easymock.IAnswer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

/**
 * Created by jon on 3/19/18.
 */
public class P2Tests {

	private static final int N_FILES = 20;
	private static final int N_REPLICAS = 20;
	@Rule
	public Timeout globalTimeout = new Timeout(22000);
	boolean err;

	@Test
	public void testServerManyClientsRegisterAllGetInnerWriteAndCommit() throws Exception {
		//Get a real server
		FileManagerServer server = new FileManagerServer();
		List<Path> files = Files.walk(ServerMain.BASEDIR).filter(Files::isRegularFile).collect(Collectors.toList());
		server.init(files);
		//Set up fake clients
		FileManagerClient[] clients = new FileManagerClient[N_REPLICAS];
		String contentToWrite = "testServerManyClientsRegisterAllGetInnerWriteAndCommit." + System.currentTimeMillis() + ".";
		HashSet<Capture<Long>> xids = new HashSet<>();
		for (int i = 0; i < N_REPLICAS; i++) {
			clients[i] = mock(FileManagerClient.class);
			xids.clear();
			for (Path p : files) {
				Capture<Long> xid = newCapture();
				xids.add(xid);
				expect(clients[i].innerWriteFile(eq(p.toString()), eq(contentToWrite + p.toString()), captureLong(xid))).andReturn(true);
				clients[i].commitTransaction(CaptureMatcher.matchesCapturedLong(xid));
				expectLastCall().once();
			}
			replay(clients[i]);
		}
		for (int i = 0; i < N_REPLICAS; i++) {
			server.registerReplica("fake hostname", 9000 + i, clients[i]);
		}
		for (Path p : files)
			server.writeFile(p.toString(), contentToWrite + p.toString());
		for (int i = 0; i < N_REPLICAS; i++) {
			server.cacheDisconnect("fake hostname", 9000 + i);
		}
		for (FileManagerClient client : clients)
			verify(client);
		HashSet<Long> actualXids = new HashSet<>();
		for (Capture<Long> xid : xids)
			if (!actualXids.add(xid.getValue()))
				fail("Expected each write would get a unique transaction ID, but observed " + xid.getValue() + " more than once");
		//Last, check that the server has all of the right files.
		FileManagerClient fake = mock(FileManagerClient.class);
		HashMap<String, String> endFiles = server.registerReplica("fake", 9, fake);
		for (Map.Entry<String, String> e : endFiles.entrySet()) {
			String expected = contentToWrite + e.getKey();
			if (!e.getValue().equals(expected))
				fail("Writes were not saved on the server, expected file content " + expected + " but got " + e.getValue());
		}
	}

	@Test
	public void testServerWriteFilePropogatesErrorsAndAborts() throws Exception {
		FileManagerServer server = new FileManagerServer();
		List<Path> files = Files.walk(ServerMain.BASEDIR).filter(Files::isRegularFile).collect(Collectors.toList());
		server.init(files);
		//Set up fake clients
		FileManagerClient[] clients = new FileManagerClient[N_REPLICAS];
		String contentToWrite = "testServerManyClientsRegisterAllGetInnerWrite." + System.currentTimeMillis() + ".";
		for (int i = 0; i < N_REPLICAS; i++) {
			clients[i] = mock(FileManagerClient.class);
			if (i == N_REPLICAS / 2)
				expect(clients[i].innerWriteFile(eq(files.get(0).toString()), eq(contentToWrite), anyLong())).andReturn(false).times(1);
			else
				expect(clients[i].innerWriteFile(eq(files.get(0).toString()), eq(contentToWrite), anyLong())).andReturn(true).times(0, 1);
			clients[i].abortTransaction(anyLong());
			expectLastCall().once();

			replay(clients[i]);
		}
		for (int i = 0; i < N_REPLICAS; i++) {
			server.registerReplica("fake hostname", 9000 + i, clients[i]);
		}
		boolean caught = false;
		try {
			server.writeFile(files.get(0).toString(), contentToWrite);
		} catch (IOException ex) {
			caught = true;
		}
		for (int i = 0; i < N_REPLICAS; i++) {
			server.cacheDisconnect("fake hostname", 9000 + i);
		}
		if (!caught)
			fail("Expected writeFile to throw an IOException if a single write failed");
		for (FileManagerClient client : clients)
			verify(client);
	}

	@Test
	public void testServerWriteFilePropogatesErrorsAndAbortsOnException() throws Exception {
		FileManagerServer server = new FileManagerServer();
		List<Path> files = Files.walk(ServerMain.BASEDIR).filter(Files::isRegularFile).collect(Collectors.toList());
		server.init(files);
		//Set up fake clients
		FileManagerClient[] clients = new FileManagerClient[N_REPLICAS];
		String contentToWrite = "testServerManyClientsRegisterAllGetInnerWrite." + System.currentTimeMillis() + ".";
		for (int i = 0; i < N_REPLICAS; i++) {
			clients[i] = mock(FileManagerClient.class);
			if (i == N_REPLICAS / 2)
				expect(clients[i].innerWriteFile(eq(files.get(0).toString()), eq(contentToWrite), anyLong())).andThrow(new IOException("Mock IO excpetion")).times(1);
			else
				expect(clients[i].innerWriteFile(eq(files.get(0).toString()), eq(contentToWrite), anyLong())).andReturn(true).times(0, 1);
			clients[i].abortTransaction(anyLong());
			expectLastCall().once();

			replay(clients[i]);
		}
		for (int i = 0; i < N_REPLICAS; i++) {
			server.registerReplica("fake hostname", 9000 + i, clients[i]);
		}
		boolean caught = false;
		try {
			server.writeFile(files.get(0).toString(), contentToWrite);
		} catch (IOException ex) {
			caught = true;
		}
		for (int i = 0; i < N_REPLICAS; i++) {
			server.cacheDisconnect("fake hostname", 9000 + i);
		}
		if (!caught)
			fail("Expected writeFile to throw an IOException if a single write failed");
		for (FileManagerClient client : clients)
			verify(client);
	}


	@Test
	public void testAbortReturnsContentToNormalAndAnotherCommitWrites() throws Exception {

		final FileManagerServer server = new FileManagerServer();
		final List<Path> files = Files.walk(ServerMain.BASEDIR).filter(Files::isRegularFile).collect(Collectors.toList());
		server.init(files);
		final long TIMEOUT = 200;
		this.err = true;

		AtomicInteger nConcurrentWrites = new AtomicInteger();
		final String fileToCrashOn = files.get(0).toString();
		AbstractFileManagerClient writerWhoAborts = new FileManagerClient(server) {
			@Override
			public boolean innerWriteFile(String file, String content, long xid) throws IOException {
				if (fileToCrashOn.equals(file))
					return false;
				return super.innerWriteFile(file, content, xid);
			}
		};
		AbstractFileManagerClient regularWriter = new FileManagerClient(server);

		String originalContent = regularWriter.readFile(files.get(0).toString());
		try {
			Thread writerThread = new Thread(() -> {
				boolean caught = false;
				try {
					server.writeFile(files.get(0).toString(), "garbage");
				} catch (IOException e) {
					caught = true;
				}
				if (!caught) {
					err = true;
					System.err.println("Expected write to abort");
				}
			});

			Thread writer2Thread = new Thread(() -> {
				try {
					server.writeFile(files.get(1).toString(), "garbage2");
				} catch (IOException e) {
					e.printStackTrace();
					err = true;
				}
			});
			writerThread.start();
			writer2Thread.start();
			writerThread.join();
			writer2Thread.join();
			assertEquals(originalContent, regularWriter.readFile(files.get(0).toString()));
			assertEquals(originalContent, writerWhoAborts.readFile(files.get(0).toString()));

			assertEquals("garbage2", regularWriter.readFile(files.get(1).toString()));
			assertEquals("garbage2", writerWhoAborts.readFile(files.get(1).toString()));

			//Last, check that the server has all of the right files.
			FileManagerClient fake = mock(FileManagerClient.class);
			HashMap<String, String> endFiles = server.registerReplica("fake", 9, fake);
			assertEquals(originalContent,endFiles.get(files.get(0).toString()));
		} finally {
			regularWriter.cleanup();
			writerWhoAborts.cleanup();
		}
	}
}
