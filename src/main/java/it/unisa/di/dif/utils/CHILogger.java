package it.unisa.di.dif.utils;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.*;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;

import java.io.File;

/**
 * The CHILogger class is a singleton class that creates a logger object that writes to a log file
 */
public class CHILogger {
    public Logger log;
    public final String TIME_STAT= "<<RT>>";
    public Constant constant = Constant.getInstance();
    public static CHILogger instance = null;

    /**
     * If the instance is null, create a new instance of CHILogger and return it
     *
     * @return The instance of the CHILogger class.
     */
    public static CHILogger getInstance() {
        if(instance == null) {
            instance = new CHILogger();
        }
        return instance;
    }

    private CHILogger() {
        File logFolder = new File(constant.getAppdir() + File.separator + "logs");
        if(!logFolder.exists()){
            logFolder.mkdirs();
        }

        String level = System.getProperty("logLevel", "WARN");

        Level l = Level.WARN;

        switch(level.toUpperCase()){
            case "DEBUG":
                l = Level.DEBUG;
                break;
            case "INFO":
                l = Level.INFO;
                break;
            case "ERROR":
                l = Level.ERROR;
                break;
            case "FATAL":
                l = Level.FATAL;
                break;
            case "TRACE":
                l = Level.TRACE;
                break;
            case "ALL":
                l = Level.ALL;
                break;
            case "OFF":
                l = Level.OFF;
                break;
            default:
                l = Level.WARN;
        }

        String logFilePath=logFolder.getAbsolutePath()+File.separator+"dif.log";
        String logFilePatternPath=logFolder.getAbsolutePath()+File.pathSeparator+"dif-%d{MM-dd-yy}.log.gz";

        ConfigurationBuilder<BuiltConfiguration> builder = ConfigurationBuilderFactory.newConfigurationBuilder();
        builder.setStatusLevel(l)
                .setConfigurationName("SCIConfiguration");

        LayoutComponentBuilder layoutBuilder = builder.newLayout("PatternLayout")
                .addAttribute("pattern", "%d [%t] %-5level: %msg%n");
        ComponentBuilder triggeringPolicy = builder.newComponent("Policies")
                .addComponent(builder.newComponent("CronTriggeringPolicy").addAttribute("schedule", "0 0 0 * * ?"))
                .addComponent(builder.newComponent("SizeBasedTriggeringPolicy").addAttribute("size", "100M"));
        AppenderComponentBuilder appenderBuilder = builder.newAppender("rolling", "RollingFile")
                .addAttribute("fileName", logFilePath)
                .addAttribute("filePattern", logFilePatternPath)
                .add(layoutBuilder)
                .addComponent(triggeringPolicy);
        builder.add(appenderBuilder);

        if(constant.isWriteMessageLogOnConsole()){
            appenderBuilder = builder.newAppender("Stdout", "CONSOLE").addAttribute("target",
                    ConsoleAppender.Target.SYSTEM_OUT);
            appenderBuilder.add(builder.newLayout("PatternLayout")
                    .addAttribute("pattern", "%d [%t] %-5level: %msg%n%throwable"));
            builder.add( appenderBuilder );
        }

        // create the new logger
        builder.add( builder.newLogger( constant.getStringLogName(), Level.ALL )
                .add( builder.newAppenderRef( "rolling" ) )
                .addAttribute( "additivity", false ) );

        builder.add( builder.newRootLogger( Level.ALL )
                .add( builder.newAppenderRef( "rolling" ) ) );

        LoggerContext ctx = Configurator.initialize(builder.build());

        log = LogManager.getLogger(constant.getStringLogName());
    }
}
