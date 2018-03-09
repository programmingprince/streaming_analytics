package com.logpoint.analytics.tsanomaly.logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import com.cloudbees.syslog.Facility;
import com.cloudbees.syslog.MessageFormat;
import com.cloudbees.syslog.Severity;
import com.cloudbees.syslog.sender.TcpSyslogMessageSender;

public class SyslogTCP {

    public static String HOST_NAME = null;
    private static final String _serviceName = "tsanomaly";
    private TcpSyslogMessageSender messageSender;

    /**
     * @return the hOST_NAME
     */
    public static String getHOST_NAME() {
        if (HOST_NAME == null) {
            try {
                HOST_NAME = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {

            }
        }
        return HOST_NAME;
    }

    public SyslogTCP() {
        messageSender = new TcpSyslogMessageSender();
        messageSender.setDefaultMessageHostname(getHOST_NAME());
        messageSender.setDefaultAppName(_serviceName);
        messageSender.setDefaultFacility(Facility.USER);
        messageSender.setDefaultSeverity(Severity.NOTICE);
        messageSender.setSyslogServerHostname("localhost");
        messageSender.setSyslogServerPort(514);
        messageSender.setMessageFormat(MessageFormat.RFC_3164);
    }

    public void sendLog(String formattedMessage) throws IOException {
        messageSender.sendMessage(formattedMessage);
    }

    public TcpSyslogMessageSender getMessageSender() {
        return this.messageSender;
    }

}
