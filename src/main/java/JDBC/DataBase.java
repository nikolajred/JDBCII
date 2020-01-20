package JDBC;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by cube on 21.01.2017.
 */
public class DataBase{
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/postgres";
    private static final String USER_NAME = "postgres";
    private static final String PASSWORD = "root";

    public static void main(String[] args) {
        addMySQLToClassPath();
        createDbUserTable();
        List<DbUser> users = new ArrayList<>();
        users.add(new DbUser("Lena", "Golovach"));
        users.add(new DbUser("Vasia", "Pupkin"));
        addUsers(users);

        System.out.println(readUsers());
    }

    private static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(DB_URL, USER_NAME, PASSWORD);
    }

    private static void addMySQLToClassPath() {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void createDbUserTable() {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS db_user("
                + "USER_ID SERIAL, "
                + "USER_NAME VARCHAR(20) NOT NULL, "
                + "LAST_NAME VARCHAR(20) NOT NULL, "
                + "PRIMARY KEY (USER_ID) "
                + ")";
        try (Connection dbConnection = getConnection();
             Statement statement = dbConnection.createStatement()) {
            // выполнить SQL запрос
            statement.execute(createTableSQL);
            System.out.println("Table \"dbuser\" is created!");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void addUsers(List<DbUser> users) {
        try (Connection connection = getConnection();
             PreparedStatement statement = connection.prepareStatement("INSERT INTO db_user(USER_NAME,LAST_NAME) VALUES(?,?)")) {
            users.forEach(dbUser -> {
                try {
                    int i = 0;
                    statement.setString(++i, dbUser.name);
                    statement.setString(++i, dbUser.lastName);
                    statement.execute();

                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private static List<DbUser> readUsers() {
        List<DbUser> result = new ArrayList<>();
        try (Connection connection = getConnection();
             Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT * FROM db_user");

            while (resultSet.next()) {
                String name = resultSet.getString("USER_NAME");
                String lastName = resultSet.getString("LAST_NAME");
                result.add(new DbUser(name, lastName));
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return result.stream().distinct().collect(Collectors.toList());
    }

    private static class DbUser {
        String name;
        String lastName;

        public DbUser(String name, String lastName) {
            this.name = name;
            this.lastName = lastName;
        }

        @Override
        public String toString() {
            return "DbUser{" +
                    "name='" + name + '\'' +
                    ", lastName='" + lastName + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            DbUser dbUser = (DbUser) o;

            if (name != null ? !name.equals(dbUser.name) : dbUser.name != null) return false;
            return lastName != null ? lastName.equals(dbUser.lastName) : dbUser.lastName == null;
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (lastName != null ? lastName.hashCode() : 0);
            return result;
        }
    }
}
