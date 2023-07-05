package rs.etf.sab.student;

import rs.etf.sab.operations.GeneralOperations;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Calendar;

public class da190101_GeneralOperationsImpl implements GeneralOperations {

    private Calendar initialTime = null;

    Connection connection = DB.getInstance().getConnection();

    @Override
    public void setInitialTime(Calendar calendar) {
        initialTime = calendar;
    }

    @Override
    public Calendar time(int i) {
        return null;
    }

    @Override
    public Calendar getCurrentTime() {
        return Calendar.getInstance();
    }

    @Override
    public void eraseAll() {
        String query = "delete from Order where 1 = 1 go" +
                "delete from Buyer where 1 = 1 go" +
                "delete from IsConnected where 1= 1 go" +
                "delete from Article where 1 = 1 go" +
                "delete from Shop where 1 = 1 go" +
                "delete from City where 1 = 1 go";

        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
