package edu.gmu.cs475.internal;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;

import org.junit.runners.model.Statement;

/**
 * Based heavily on Junit4 org.junit.internal.runners.statements.FailOnTimeout
 * @author jon
 *
 */
public class FailOnTimeoutIfDeadlocked extends Statement {
    private final Statement fOriginalStatement;

    private final long fTimeout;

    
    public FailOnTimeoutIfDeadlocked(Statement originalStatement, long timeout) {
        fOriginalStatement = originalStatement;
        fTimeout = timeout;
    }

    @Override
    public void evaluate() throws Throwable {
        StatementThread thread = evaluateStatement();
        if (!thread.fFinished) {
            throwExceptionForUnfinishedThread(thread);
        }
    }

	private final ThreadMXBean mbean = ManagementFactory.getThreadMXBean();
    private StatementThread evaluateStatement() throws InterruptedException {
        StatementThread thread = new StatementThread(fOriginalStatement);
        thread.start();
        thread.join(fTimeout);
        if (!thread.fFinished) {
            thread.recordStackTrace();
        }

        long[] deadlockedThreadIds = mbean.findDeadlockedThreads();

		if (deadlockedThreadIds != null) {
			System.out.println("Deadlock found!");
			ThreadInfo[] threadInfos = mbean.getThreadInfo(deadlockedThreadIds);
			System.out.println(Arrays.toString(threadInfos));
			thread.deadlocked = true;
		}
        thread.interrupt();
        return thread;
    }

    private void throwExceptionForUnfinishedThread(StatementThread thread)
            throws Throwable {
        if (thread.fExceptionThrownByOriginalStatement != null) {
            throw thread.fExceptionThrownByOriginalStatement;
        } else {
            throwTimeoutException(thread);
        }
    }

    private void throwTimeoutException(StatementThread thread) throws Exception {
		if (thread.deadlocked) {
			AssertionError exception = new AssertionError(String.format("Test deadlocked!", fTimeout));
			exception.setStackTrace(thread.getRecordedStackTrace());
			throw exception;
		}
		Exception exception = new Exception(String.format("test timed out after %d milliseconds. No obvious deadlock found.", fTimeout));
		exception.setStackTrace(thread.getRecordedStackTrace());
		throw exception;
	}

    private static class StatementThread extends Thread {
        private final Statement fStatement;

        private boolean fFinished = false;

        public boolean deadlocked = false;
        
        private Throwable fExceptionThrownByOriginalStatement = null;

        private StackTraceElement[] fRecordedStackTrace = null;

        public StatementThread(Statement statement) {
            fStatement = statement;
        }

        public void recordStackTrace() {
            fRecordedStackTrace = getStackTrace();
        }

        public StackTraceElement[] getRecordedStackTrace() {
            return fRecordedStackTrace;
        }

        @Override
        public void run() {
            try {
                fStatement.evaluate();
                fFinished = true;
            } catch (InterruptedException e) {
                // don't log the InterruptedException
            } catch (Throwable e) {
                fExceptionThrownByOriginalStatement = e;
            }
        }
    }
}