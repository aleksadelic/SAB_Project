package rs.etf.sab.student;

import rs.etf.sab.operations.ShopOperations;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class da190101_ShopOperationsImpl implements ShopOperations {

    Connection connection = DB.getInstance().getConnection();

    @Override
    public int createShop(String shopName, String cityName) {
        String query = "insert into Shop (Name, IdCity) values (?, (select IdCity from City where Name = ?))";
        try (PreparedStatement ps = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, shopName);
            ps.setString(2, cityName);
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
    public int setCity(int idShop, String cityName) {
        String query = "update Shop set IdCity = (select IdCity from City where Name = ?) where idShop = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setString(1, cityName);
            ps.setInt(2, idShop);
            return ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public int getCity(int idShop) {
        String query = "select IdCity from Shop where IdShop = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idShop);
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
    public int setDiscount(int idShop, int discount) {
        String query = "update Shop set Discount = ? where IdShop = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, discount);
            ps.setInt(2, idShop);
            return ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public int increaseArticleCount(int idArticle, int increment) {
        String query = "update Article set Quantity = Quantity + ? where IdArt = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, increment);
            ps.setInt(2, idArticle);
            ps.executeUpdate();
            return getArticleCount(idArticle);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return -1;
    }

    @Override
    public int getArticleCount(int idArticle) {
        String query = "select Quantity from Article where IdArt = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idArticle);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public List<Integer> getArticles(int idShop) {
        List<Integer> list = new ArrayList<>();
        String query = "select IdArt from Article where IdShop = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idShop);
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
    public int getDiscount(int idShop) {
        String query = "select Discount from Shop where IdShop = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idShop);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static void main(String[] args) {
        da190101_ShopOperationsImpl obj = new da190101_ShopOperationsImpl();
        obj.setDiscount(1, 5);
        System.out.println(obj.getDiscount(1));
    }

}
