package test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class Main {

    public static void main(String[] args) throws ClassNotFoundException, NoSuchMethodException,
                                                  InvocationTargetException, IllegalAccessException {
        Class<?> testClass = Class.forName(args[0]);
        Method main = testClass.getMethod("main", String[].class);
        main.invoke(null, (Object)new String[] {});
    }
}
