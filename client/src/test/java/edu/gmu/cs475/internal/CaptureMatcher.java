package edu.gmu.cs475.internal;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IArgumentMatcher;

/**
 * Created by jon on 3/19/18.
 */
public class CaptureMatcher<T> implements IArgumentMatcher {

    private Capture<T> capture;
    public CaptureMatcher(Capture<T> capture){
        this.capture = capture;
    }
    @Override
    public boolean matches(Object o) {
        return capture.hasCaptured() && capture.getValue().equals(o);
    }

    @Override
    public void appendTo(StringBuffer stringBuffer) {
        //stringBuffer.append("Expected previously captured ");
        stringBuffer.append(capture.getValue().toString());
    }

    public static <T> T matchesCaptured(Capture<T> in){
        EasyMock.reportMatcher(new CaptureMatcher<T>(in));
        return null;
    }
    public static int matchesCapturedInt(Capture<Integer> in)
    {
        EasyMock.reportMatcher(new CaptureMatcher<Integer>(in));
        return 0;
    }
    public static long matchesCapturedLong(Capture<Long> in)
    {
        EasyMock.reportMatcher(new CaptureMatcher<Long>(in));
        return 0;
    }
}
