package test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class Main {

    public static void main(String[] args) throws ClassNotFoundException, NoSuchMethodException,
                                                  InvocationTargetException, IllegalAccessException {
        Class<?> testClass = Class.forName(args[0]);
        Method main = testClass.getMethod("main", String[].class);
        int rvLen = args.length - 1;
        String[] newArgs = new String [rvLen];
        System.arraycopy(args, 1, newArgs, 0, rvLen);
        main.invoke(null, (Object) newArgs);
    }
}
