package rs.etf.sab.student;

import rs.etf.sab.operations.GeneralOperations;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class da190101_GeneralOperationsImpl implements GeneralOperations {

    private static Calendar time = null;
    private int day = 0;
    public HashMap<Integer, int[][]> orderMap = new HashMap<>();

    static da190101_GeneralOperationsImpl GENERAL_OPERATIONS = new da190101_GeneralOperationsImpl();

    Connection connection = DB.getInstance().getConnection();

    @Override
    public void setInitialTime(Calendar calendar) {
        time = Calendar.getInstance();
        time.clear();
        time.setTime(calendar.getTime());
        day = 0;
    }

    @Override
    public Calendar time(int days) {
        time.add(Calendar.DAY_OF_MONTH, days);
        day += days;

        // update orders locations
        for (Map.Entry<Integer, int[][]> entry: GENERAL_OPERATIONS.orderMap.entrySet()) {
            int idOrder = entry.getKey();
            int[][] locations = entry.getValue();

            int ind = 0;
            while (ind < locations[0].length && day >= locations[0][ind]) {
                ind++;
            }
            int currLocation = locations[1][ind - 1];

            if (ind == locations[1].length) {
                int offset = day - locations[0][ind - 1];
                Calendar receivedTime = Calendar.getInstance();
                receivedTime.clear();
                receivedTime.setTime(time.getTime());
                receivedTime.add(Calendar.DAY_OF_MONTH, -offset);
                da190101_TransactionOperationsImpl.TRANSACTION_OPERATIONS.createTransactions(idOrder, receivedTime);
            }

            String query = "update [Order] set Location = ? where IdOrd = ?";
            try (PreparedStatement ps = connection.prepareStatement(query)) {
                ps.setInt(1, currLocation);
                ps.setInt(2, idOrder);
                ps.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return time;
    }

    @Override
    public Calendar getCurrentTime() {
        return time;
    }

    private String[] tables = new String[]{ "SystemTransaction", "ShopTransaction", "BuyerTransaction", "[Transaction]",
            "Item", "[Order]", "Article", "Buyer", "Shop", "IsConnected", "City"};

    @Override
    public void eraseAll() {
        for (int i = 0; i < tables.length; i++) {
            try (PreparedStatement ps1 = connection.prepareStatement("DISABLE TRIGGER ALL ON " + tables[i]);
                 PreparedStatement ps2 = connection.prepareStatement("ALTER TABLE " + tables[i] + " NOCHECK CONSTRAINT ALL");
                 PreparedStatement ps3 = connection.prepareStatement("SET QUOTED_IDENTIFIER ON; DELETE FROM " + tables[i]);
                 PreparedStatement ps4 = connection.prepareStatement("ALTER TABLE " + tables[i] + " WITH CHECK CHECK CONSTRAINT ALL");
                 PreparedStatement ps5 = connection.prepareStatement("ENABLE TRIGGER ALL ON " + tables[i]);) {
                ps1.execute();
                ps2.execute();
                ps3.execute();
                ps4.execute();
                ps5.execute();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        da190101_GeneralOperationsImpl genOp = new da190101_GeneralOperationsImpl();
        genOp.eraseAll();
    }
}
