package com.HbaseIA.TwitBase;

import com.HbaseIA.TwitBase.model.User;
import com.HbaseIA.TwitBase.model.UsersDAO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;


public class UserTools {
    public static final String usage =
            "UsersTool action ...\n" +
                    "  help - print this message and exit.\n" +
                    "  add user name email password" +
                    " - add a new user.\n" +
                    "  get user - retrieve a specific user. \n" +
                    "  list - list all installed users.";

    public static void main(String[] args) throws IOException {
        if (args.length == 0 || "help".equals(args[0])) {
            System.out.println(usage);
            System.exit(0);
        }
        Configuration configuration = new Configuration();

        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "10.104.2.219");
        Connection connection = ConnectionFactory.createConnection(configuration);
        UsersDAO dao = new UsersDAO(connection);

        if ("get".equals(args[0])) {
            System.out.println("Getting user " + args[1]);
            User u = dao.getUser(args[1]);
            System.out.println("Successfully added user " + u);
        }
        if ("list".equals(args[0])) {
            for (User u : dao.getUsers()) {
                System.out.println(u);
            }
        }
        if ("add".equals(args[0])) {
            System.out.println("Adding user...");
            dao.addUser(args[1],args[2],args[3],args[4]);
            User user = dao.getUser(args[1]);
            System.out.println("Successfully added user "+ user);
        }
        connection.close();
    }
}
