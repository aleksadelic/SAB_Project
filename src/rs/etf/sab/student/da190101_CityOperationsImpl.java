package rs.etf.sab.student;

import rs.etf.sab.operations.CityOperations;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class da190101_CityOperationsImpl implements CityOperations {

    Connection connection = DB.getInstance().getConnection();

    @Override
    public int createCity(String name) {
        String query = "insert into City (name) values (?)";
        try (PreparedStatement ps = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, name);
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
    public List<Integer> getCities() {
        List<Integer> list = new ArrayList<>();
        String query = "select IdCity from City";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
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
    public int connectCities(int idCity1, int idCity2, int distance) {
        String query = "insert into IsConnected values (?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idCity1);
            ps.setInt(2, idCity2);
            ps.setInt(3, distance);
            return ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public List<Integer> getConnectedCities(int idCity) {
        List<Integer> list = new ArrayList<>();
        String query1 = "select IdCity2 from IsConnected where IdCity1 = ?";
        String query2 = "select IdCity1 from IsConnected where IdCity2 = ?";
        try (PreparedStatement ps1 = connection.prepareStatement(query1);
             PreparedStatement ps2 = connection.prepareStatement(query2)) {
            ps1.setInt(1, idCity);
            ResultSet rs = ps1.executeQuery();
            while (rs.next()) {
                list.add(rs.getInt(1));
            }
            ps2.setInt(1, idCity);
            rs = ps2.executeQuery();
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
    public List<Integer> getShops(int idCity) {
        List<Integer> list = new ArrayList<>();
        String query = "select IdShop from Shop where IdCity = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idCity);
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

    public static void main(String[] args) {
        da190101_CityOperationsImpl obj = new da190101_CityOperationsImpl();
        List<Integer> shops = obj.getShops(1);
        for (int idShop: shops) {
            System.out.println(idShop);
        }
    }
}
