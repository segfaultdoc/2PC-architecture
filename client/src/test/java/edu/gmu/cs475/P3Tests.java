package edu.gmu.cs475;

import edu.gmu.cs475.internal.NoOpFileManagerClient;
import edu.gmu.cs475.internal.ServerMain;
import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Created by jon on 3/19/18.
 */
public class P3Tests {

	private static final int N_FILES = 20;
	private static final int N_REPLICAS = 20;
	@Rule
	public Timeout globalTimeout = new Timeout(22000);
	boolean err;


	@Test
	public void testClientEchoAllLocksThenStartsTransactionThenWritesAndCommits() throws Exception {
		IMocksControl mocker = EasyMock.createControl();
		mocker.resetToStrict();
		IFileManagerServer server = mock(IFileManagerServer.class);
		try {
			expect(server.registerClient(anyString(), anyInt())).andAnswer(() -> {
				HashMap<String, String> ret = new HashMap<String, String>();
				for (int i = 0; i < N_FILES; i++)
					ret.put("File" + i, "Contents " + i);
				return ret;
			}).once();
			for (int i = 0; i < N_FILES; i++)
				expect(server.lockFile(anyString(), anyBoolean())).andReturn(40L + i).once();
			expect(server.startNewTransaction()).andReturn(4L).once();
			for (int i = 0; i < N_FILES; i++)
				expect(server.writeFileInTransaction("File" + i, "foo", 4L)).andReturn(true).once();
			server.issueCommitTransaction(4L);
			expectLastCall().once();
			server.unLockFile(anyString(), anyLong(), anyBoolean());
			expectLastCall().times(N_FILES);

			server.cacheDisconnect(anyString(), anyInt());
			expectLastCall().once();
			replay(server);

			FileManagerClient client = new FileManagerClient(server);
			try {
				client.echoToAllFiles("foo");
			} finally {
				client.cleanup();
			}
			verify(server);
		} finally {
			mocker.resetToDefault();
		}
	}

	@Test
	public void testClientCatAllLocksThenReadsUnlocks() throws Exception {
		IMocksControl mocker = EasyMock.createControl();
		mocker.resetToStrict();
		IFileManagerServer server = mock(IFileManagerServer.class);
		try {
			expect(server.registerClient(anyString(), anyInt())).andAnswer(() -> {
				HashMap<String, String> ret = new HashMap<String, String>();
				for (int i = 0; i < N_FILES; i++)
					ret.put("File" + i, "Contents " + i);
				return ret;
			}).once();
			for (int i = 0; i < N_FILES; i++)
				expect(server.lockFile(anyString(), anyBoolean())).andReturn(40L + i).once();
			server.unLockFile(anyString(), anyLong(), anyBoolean());
			expectLastCall().times(N_FILES);

			server.cacheDisconnect(anyString(), anyInt());
			expectLastCall().once();
			replay(server);

			FileManagerClient client = new FileManagerClient(server);
			try {
				client.catAllFiles();
			} finally {
				client.cleanup();
			}
			verify(server);
		} finally {
			mocker.resetToDefault();
		}
	}

	@Test
	public void testClientLocksThenStartsTransactionThenWritesAndAbortsOnFailure() throws Exception {
		IMocksControl mocker = EasyMock.createControl();
		mocker.resetToStrict();
		IFileManagerServer server = mock(IFileManagerServer.class);
		try {
			expect(server.registerClient(anyString(), anyInt())).andAnswer(() -> {
				HashMap<String, String> ret = new HashMap<String, String>();
				for (int i = 0; i < N_FILES; i++)
					ret.put("File" + i, "Contents " + i);
				return ret;
			}).once();
			for (int i = 0; i < N_FILES; i++)
				expect(server.lockFile(anyString(), anyBoolean())).andReturn(40L + i).once();
			expect(server.startNewTransaction()).andReturn(4L).once();
			for (int i = 0; i < N_FILES - 1; i++)
				expect(server.writeFileInTransaction("File" + i, "foo", 4L)).andReturn(true).once();
			expect(server.writeFileInTransaction("File" + (N_FILES - 1), "foo", 4L)).andReturn(false).once();
			server.issueAbortTransaction(4L);
			expectLastCall().once();
			server.unLockFile(anyString(), anyLong(), anyBoolean());
			expectLastCall().times(N_FILES);

			server.cacheDisconnect(anyString(), anyInt());
			expectLastCall().once();
			replay(server);

			FileManagerClient client = new FileManagerClient(server);
			try {
				client.echoToAllFiles("foo");
			} finally {
				client.cleanup();
			}
			verify(server);
		} finally {
			mocker.resetToDefault();
		}
	}

	@Test
	public void testServerWriteFileInTransactionDoesNotLock() throws Exception {
		FileManagerServer server = new FileManagerServer() {
			@Override
			public long lockFile(String name, boolean forWrite) throws RemoteException, NoSuchFileException {
				fail("lock file should not be called by writeFileInTransaction");
				return super.lockFile(name, forWrite);
			}

			@Override
			public void unLockFile(String name, long stamp, boolean forWrite) throws RemoteException, NoSuchFileException, IllegalMonitorStateException {
				fail("lock file should not be called by writeFileInTransaction");
			}
		};
		FileManagerClient client = mock(FileManagerClient.class);

		expect(client.innerWriteFile(anyString(), anyString(), anyLong())).andReturn(true).once();
		replay(client);
		List<Path> files = Files.walk(ServerMain.BASEDIR).filter(Files::isRegularFile).collect(Collectors.toList());
		try {
			server.init(files);

			server.registerReplica("foo", 90004, client);

			String file = files.get(0).toString();
			long xid = server.startNewTransaction();
			server.writeFileInTransaction(file, "zz", xid);
		} finally {
			server.cacheDisconnect("foo", 90004);
		}
		verify(client);
	}

	@Test
	public void testServerCommitReachesAllClients() throws Exception {

		List<Path> files = Files.walk(ServerMain.BASEDIR).filter(Files::isRegularFile).collect(Collectors.toList());
		FileManagerServer server = new FileManagerServer();
		server.init(files);
		FileManagerClient[] clients = new FileManagerClient[N_REPLICAS];
		long xid = server.startNewTransaction();
		String uniqueContent = "testServerCommitReachesAllClients." + System.currentTimeMillis();
		for (int i = 0; i < N_REPLICAS; i++) {
			clients[i] = mock(FileManagerClient.class);
			for (int j = 0; j < files.size(); j++) {
				expect(clients[i].innerWriteFile(files.get(j).toString(), uniqueContent + files.get(j).toString(), xid)).andReturn(true).once();
			}
			clients[i].commitTransaction(xid);
			expectLastCall().once();
			server.registerReplica("foo", 9000 + i, clients[i]);
			replay(clients[i]);
		}

		try {
			for (Path p : files) {
				String file = p.toString();
				server.writeFileInTransaction(file, uniqueContent + file, xid);
			}
			server.issueCommitTransaction(xid);
		} finally {
		}
		//Make sure that all of the writes went through on the server too
		FileManagerClient fake = mock(FileManagerClient.class);
		HashMap<String, String> endFiles = server.registerReplica("fake", 9, fake);
		for (Map.Entry<String, String> e : endFiles.entrySet()) {
			String expected = uniqueContent + e.getKey();
			if (!e.getValue().equals(expected))
				fail("Writes were not committed on the server, expected file content " + expected + " but got " + e.getValue());
		}
		for (FileManagerClient client : clients)
			verify(client);
	}

	@Test
	public void testServerThatClientCantRegisterDuringCommit() throws Exception {
		err = false;
		dangling.clear();
		List<Path> files = Files.walk(ServerMain.BASEDIR).filter(Files::isRegularFile).collect(Collectors.toList());
		FileManagerServer server = new FileManagerServer();
		server.init(files);
		AtomicInteger completedCommits = new AtomicInteger();
		FirstGuyLeavesWhileWritingClient c1 = new FirstGuyLeavesWhileWritingClient(server,9004,completedCommits);
		FirstGuyLeavesWhileWritingClient c2 = new FirstGuyLeavesWhileWritingClient(server,9005,completedCommits);

		server.registerReplica("fake client", 9004, c1);
		server.registerReplica("fake client", 9005, c2);

		long id = server.startNewTransaction();
		server.writeFileInTransaction(files.get(0).toString(),"someContent",id);
		server.issueCommitTransaction(id);

		for(Thread t: dangling){
			t.join(100);
		}
		assertFalse(err);
	}
	HashSet<Thread> dangling = new HashSet<>();

	class FirstGuyLeavesWhileWritingClient extends NoOpFileManagerClient {
		AtomicInteger completedCommits;
		AbstractFileManagerServer server;
		int port;

		FirstGuyLeavesWhileWritingClient(AbstractFileManagerServer server, int port, AtomicInteger completedCommits) {
			super(server);
			this.completedCommits = completedCommits;
			this.server = server;
			this.port = port;
		}

		@Override
		public boolean innerWriteFile(String file, String content, long xid) throws IOException {
			return true;
		}

		@Override
		public void commitTransaction(long id) throws IOException {
			Thread t = new Thread(() -> {
				AbstractFileManagerClient newClient = createNiceMock(AbstractFileManagerClient.class);
				try {
					expect(newClient.innerWriteFile(anyString(),anyString(),anyLong())).andReturn(true);
					replay(newClient);
				} catch (IOException e) {
					e.printStackTrace();
				}
				try {
					//Concurrency: this method can't return until the writes are all completed
					server.registerReplica("fake", port+10, newClient);
				} catch (IOException e) {
					e.printStackTrace();
				}
				if (completedCommits.get() != 2) {
					err = true;
					System.err.println("Error: We were able to add a replica while a write was in progress");
				}
			});
			t.start();
			dangling.add(t);
			try {
				Thread.sleep(300);
				completedCommits.incrementAndGet();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
