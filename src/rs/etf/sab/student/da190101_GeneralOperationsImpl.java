package rs.etf.sab.student;

import rs.etf.sab.operations.GeneralOperations;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;

public class da190101_GeneralOperationsImpl implements GeneralOperations {

    private Calendar time = null;

    static GeneralOperations GENERAL_OPERATIONS = new da190101_GeneralOperationsImpl();

    Connection connection = DB.getInstance().getConnection();

    @Override
    public void setInitialTime(Calendar calendar) {
        time = calendar;
    }

    @Override
    public Calendar time(int days) {
        time.add(Calendar.DATE, days);
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
