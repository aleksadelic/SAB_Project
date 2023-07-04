package rs.etf.sab.student;

import rs.etf.sab.operations.BuyerOperations;

import javax.xml.transform.Result;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class da190101_BuyerOperationsImpl implements BuyerOperations {

    Connection connection = DB.getInstance().getConnection();

    @Override
    public int createBuyer(String name, int idCity) {
        String query = "insert into Buyer (Name, IdCity) values (?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, name);
            ps.setInt(2, idCity);
            ps.executeUpdate();
            ResultSet rs = ps.getGeneratedKeys();
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public int setCity(int idBuyer, int idCity) {
        String query = "update Buyer set IdCity = ? where IdBuy = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idCity);
            ps.setInt(2, idBuyer);
            return ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public int getCity(int idBuyer) {
        String query = "select IdCity from Buyer where IdBuy = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idBuyer);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public BigDecimal increaseCredit(int idBuyer, BigDecimal credit) {
        BigDecimal oldCredit = getCredit(idBuyer);
        String query = "update Buyer set credit = credit + ? where IdBuy = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setDouble(1, credit.doubleValue());
            ps.setInt(2, idBuyer);
            ps.executeUpdate();
            return new BigDecimal(oldCredit.doubleValue() + credit.doubleValue());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new BigDecimal(-1);
    }

    @Override
    public int createOrder(int idBuyer) {
        String query = "insert into [Order] (IdBuy) values (?)";
        try (PreparedStatement ps = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {
            ps.setInt(1, idBuyer);
            ps.executeUpdate();
            ResultSet rs = ps.getGeneratedKeys();
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public List<Integer> getOrders(int idBuyer) {
        List<Integer> list = new ArrayList<>();
        String query = "select IdOrd from [Order] where IdBuy = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idBuyer);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                list.add(rs.getInt(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }

        return list;
    }

    @Override
    public BigDecimal getCredit(int idBuyer) {
        String query = "select Credit from Buyer where IdBuy = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idBuyer);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return new BigDecimal(rs.getDouble(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new BigDecimal(-1);
    }

    public static void main(String[] args) {
        da190101_BuyerOperationsImpl obj = new da190101_BuyerOperationsImpl();
        //obj.createBuyer("Aleksa", 1);
        //obj.setCity(1, 2);
        //System.out.println(obj.increaseCredit(1, new BigDecimal(1000)));
        //System.out.println(obj.getCity(1));
        //System.out.println(obj.createOrder(1));
        List<Integer> orders = obj.getOrders(1);
        for (int order: orders) {
            System.out.println(order);
        }
    }
}
