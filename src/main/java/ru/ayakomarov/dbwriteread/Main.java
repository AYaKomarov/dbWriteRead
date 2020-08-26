package ru.ayakomarov.dbwriteread;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Main {

    private final static String URL = "jdbc:postgresql://localhost:5432/autoorder";
    private final static String USERNAME = "autoorder";
    private final static String PASSWORD = "autoorder";

    private final static String CREATE_TABLE = "create table if not exists writer_reader_table (id bigserial, value text)";
    private final static String DROP_TABLE = "drop table writer_reader_table";
    private final static String[] GENERATED_KEY = { "id" };

    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
        Class.forName("org.postgresql.Driver");
        try (Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD)) {

            createTable(connection);

            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            while (true) {
                String stringFromConsole = reader.readLine();
                String[] splited = stringFromConsole.split("\\s+");
                if (splited.length == 0) {
                    continue;
                }
                if (splited[0].equals("create")) {
                    if (splited.length < 2) {
                        System.out.println("write something after \"create\"");
                        continue;
                    } else if (splited.length > 2) {
                        System.out.println("write only one parameter after \"create\"");
                        continue;
                    }
                    write(connection, splited[1]);
                } else if (splited[0].equals("get")) {
                    if (splited.length < 2) {
                        System.out.println("use id number after \"get\"");
                        continue;
                    } else if (splited.length > 2) {
                        System.out.println("use only id number after \"get\"");
                        continue;
                    }
                    read(connection, splited[1]);
                } else if (splited[0].equals("exit")) {
                    break;
                } else {
                    System.out.println("Not found command: " + splited[0]);
                }
            }

            dropTable(connection);
        }
    }

    private static void createTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(CREATE_TABLE);
        }
    }

    private static void write(Connection connection, String value) throws SQLException {
        String sql = "insert into writer_reader_table (value) values ('" + value + "')";
        try (PreparedStatement statement = connection.prepareStatement(sql, GENERATED_KEY)){
            statement.executeUpdate();
            try (ResultSet generatedKeys = statement.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    System.out.println(generatedKeys.getLong(1));
                }
            }
        }
    }

    private static void read(Connection connection, String idStr) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            int id = Integer.parseInt(idStr);
            String sql = "select * from writer_reader_table where id = " + id;
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                String value = resultSet.getString("value");
                System.out.println(value);
            }
            resultSet.close();
        } catch (NumberFormatException e) {
            System.out.println(idStr + " is not number");
        }
    }

    private static void dropTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(DROP_TABLE);
        }
    }
}
