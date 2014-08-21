package com.inmobi.grill.server.query;

import com.inmobi.grill.api.query.QueryStatus;
import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.api.events.AsyncEventListener;
import com.inmobi.grill.server.api.events.GrillEventService;
import com.inmobi.grill.server.api.query.QueryContext;
import com.inmobi.grill.server.api.query.QueryEnded;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.util.Date;
import java.util.Properties;

/**
 * User: rajat.khandelwal
 * Date: 21/08/14
 */
public class QueryEndNotifier extends AsyncEventListener<QueryEnded> {
  private final QueryExecutionServiceImpl queryService;
  private final GrillEventService eventService;
  public static final Log LOG = LogFactory.getLog(QueryEndNotifier.class);
  private final HiveConf conf;

  public QueryEndNotifier(QueryExecutionServiceImpl queryService,
    GrillEventService eventService, HiveConf hiveConf) {
    this.queryService = queryService;
    this.eventService = eventService;
    this.conf = hiveConf;
  }

  @Override
  public void process(QueryEnded event) {
    String from = conf.get(GrillConfConstants.GRILL_MAIL_FROM_ADDRESS);
    String host = conf.get(GrillConfConstants.GRILL_MAIL_HOST);
    String port = conf.get(GrillConfConstants.GRILL_MAIL_PORT);
    int mailSmtpTimeout = Integer.parseInt(conf.get(GrillConfConstants.GRILL_MAIL_SMTP_TIMEOUT,
      GrillConfConstants.GRILL_MAIL_DEFAULT_SMTP_TIMEOUT));
    int mailSmtpConnectionTimeout = Integer.parseInt(conf.get(GrillConfConstants.GRILL_MAIL_SMTP_CONNECTIONTIMEOUT,
      GrillConfConstants.GRILL_MAIL_DEFAULT_SMTP_CONNECTIONTIMEOUT));

    QueryContext queryContext = queryService.getQueryContext(event.getQueryHandle());
    String mailMessage = queryContext.getStatus().toString();

    sendMail(host, port, from, event.getUser(), "", event.getMailSubject(), mailMessage,
      mailSmtpTimeout, mailSmtpConnectionTimeout);
  }

  public static int sendMail(String host, String port, String from, String to, String cc, String subject,
    String mailMessage, int mailSmtpTimeout, int mailSmtpConnectionTimeout) {
    Properties props = System.getProperties();
    props.put("mail.smtp.host", host);
    props.put("mail.smtp.port", port);
    props.put("mail.smtp.timeout", mailSmtpTimeout);
    props.put("mail.smtp.connectiontimeout", mailSmtpConnectionTimeout);
    Session session = Session.getDefaultInstance(props, null);
    try {
      MimeMessage message = new MimeMessage(session);
      message.setFrom(new InternetAddress(from));
      message.setRecipient(Message.RecipientType.TO, new InternetAddress(
        to));
      message.setRecipient(Message.RecipientType.CC, new InternetAddress(
        cc));
      message.setSubject(subject);
      message.setSentDate(new Date());

      MimeBodyPart messagePart = new MimeBodyPart();
      messagePart.setText(mailMessage);
      Multipart multipart = new MimeMultipart();

      multipart.addBodyPart(messagePart);
      message.setContent(multipart);
      Transport.send(message);
    } catch (MessagingException e) {
      LOG.error("Error while sending mail " , e);
      return -1;
    } catch (Exception e) {
      LOG.error("Error while sending mail " , e);
      return -1;
    }
    return 1;
  }
}
