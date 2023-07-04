package it.unisa.di.dif.filter;

import java.lang.reflect.InvocationTargetException;

public abstract class FilterFactory {
    public static Filter getDefaultFilter() {
        return new Multirisoluzione();
    }

    public static Filter getFilterFromEnviroment() throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException{
        return (Filter) Class.forName(System.getProperty("if.unisa.di.dif.filter")).getDeclaredConstructor().newInstance();
    }

    public static Filter getFilterByClass(String className) throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        return (Filter) Class.forName(className).getDeclaredConstructor().newInstance();
    }
}
