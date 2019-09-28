package com.xyzcorp;

import io.vavr.collection.Stream;
import io.vavr.collection.Traversable;

import java.sql.*;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class MyDatabaseRunner {
    public static void main(String[] args) throws InterruptedException,
        SQLException {

        Connection conn =
            DriverManager.getConnection(
                "jdbc:mysql://localhost/kafka_orders?" +
                    "user=kafka_user&password=kafka_stuff" +
                    "&useUnicode=true&useJDBCCompliantTimezoneShift=true" +
                    "&serverTimezone=UTC");

        String stateString =
            "AK,AL,AZ,AR,CA,CO,CT,DE,FL,GA," +
                "HI,ID,IL,IN,IA,KS,KY,LA,ME,MD," +
                "MA,MI,MN,MS,MO,MT,NE,NV,NH,NJ," +
                "NM,NY,NC,ND,OH,OK,OR,PA,RI,SC," +
                "SD,TN,TX,UT,VT,VA,WA,WV,WI,WY";

        String[] creditCardTypes =
            new String[]{"VISA", "MASTERCARD", "AMERICAN EXPRESS"};

        Random random = new Random();
        AtomicBoolean done = new AtomicBoolean(false);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            done.set(true);
        }));

        int orderID = getLatestOrderId(conn);

        while (!done.get()) {
            orderID ++;
            String[] states = stateString.split(",");
            int index = random.nextInt(states.length - 1);
            String state = states[index];
            int amount = random.nextInt(99999) + 1;
            Gender gender = random.nextBoolean() ? Gender.MALE : Gender.FEMALE;
            float discount = random.nextInt(20) / 100.0f;
            Shipping shippingChoice = Shipping.values()[random.nextInt(2)];
            String creditCard = getCreditCard(random);
            String creditCardType = creditCardTypes[random.nextInt(creditCardTypes.length)];

            PreparedStatement preparedStatement = conn.prepareStatement(
                "INSERT INTO kafka_order " +
                "(order_id, state, shipping_choice, gender, " +
                "credit_card, credit_card_type, amount, discount) " +
                "values (?, ?, ?, ?, ?, ?, ?, ?)");

            preparedStatement.setInt(1, orderID);
            preparedStatement.setString(2, state);
            preparedStatement.setString(3, shippingChoice.name());
            preparedStatement.setString(4, gender.name());
            preparedStatement.setString(5, creditCard);
            preparedStatement.setString(6, creditCardType);
            preparedStatement.setDouble(7, amount);
            preparedStatement.setDouble(8, discount);

            boolean result = preparedStatement.execute();
            System.out.printf("Query result (%b) for order %d%n", result, orderID);

            Thread.sleep(random.nextInt(30000 - 5000) + 5000);
        }
    }

    private static String getCreditCard(Random random) {
        return Stream.fill(16, () -> random.nextInt(10))
                     .sliding(4,4)
                     .map(Traversable::mkString)
                     .mkString("-");
    }

    private static int getLatestOrderId(Connection conn) throws SQLException {
        PreparedStatement preparedStatement = conn.prepareStatement("SELECT " +
            "max(order_id) as max_order_id from kafka_order");
        ResultSet rs = preparedStatement.executeQuery();
        return (rs.next()) ? rs.getInt("max_order_id") : 0;
    }
}
