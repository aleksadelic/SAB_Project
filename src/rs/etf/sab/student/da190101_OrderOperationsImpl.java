package rs.etf.sab.student;

import rs.etf.sab.operations.OrderOperations;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class da190101_OrderOperationsImpl implements OrderOperations {

    Connection connection = DB.getInstance().getConnection();

    @Override
    public int addArticle(int idOrder, int idArticle, int count) {
        String query = "insert into Item (IdOrd, IdArt, Quantity) values (?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {
            ps.setInt(1, idOrder);
            ps.setInt(2, idArticle);
            ps.setInt(3, count);

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
    public int removeArticle(int idOrder, int idArticle) {
        String query = "Delete from Item where IdOrd = ? and IdArt = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idOrder);
            ps.setInt(2, idArticle);

            return ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public List<Integer> getItems(int idOrder) {
        List<Integer> list = new ArrayList<>();
        String query = "select IdItem from Item where IdOrd = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idOrder);
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
    public int completeOrder(int i) {
        return 0;
    }

    @Override
    public BigDecimal getFinalPrice(int idOrder) {
        String query = "select Item.IdArt, Item.Quantity, Price from Item Join Article on Item.IdArt = Article.IdArt where IdItem = ?";
        double sum = 0;
        List<Integer> items = getItems(idOrder);
        for (int item: items) {
            try (PreparedStatement ps = connection.prepareStatement(query)) {
                ps.setInt(1, item);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    sum += rs.getInt(2) * rs.getDouble(3);
                }
            } catch (SQLException e) {
                e.printStackTrace();
                return null;
            }
        }
        return new BigDecimal(sum).subtract(getDiscountSum(idOrder));
    }

    @Override
    public BigDecimal getDiscountSum(int idOrder) {
        String query = "select Item.IdArt, Item.Quantity, Price, Discount from Item Join Article on Item.IdArt = Article.IdArt" +
                " join Shop on Article.IdShop = Shop.IdShop where IdItem = ?";
        double sum = 0;
        List<Integer> items = getItems(idOrder);
        for (int item: items) {
            try (PreparedStatement ps = connection.prepareStatement(query)) {
                ps.setInt(1, item);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    sum += rs.getInt(2) * rs.getDouble(3) * rs.getInt(4) / 100;
                }
            } catch (SQLException e) {
                e.printStackTrace();
                return null;
            }
        }
        return new BigDecimal(sum);
    }

    @Override
    public String getState(int idOrder) {
        String query = "select Status from Order where IdOrd = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idOrder);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getString(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Calendar getSentTime(int idOrder) {
        String query = "select TimeSent from Order where IdOrd = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idOrder);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(rs.getDate(1));
                return calendar;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Calendar getRecievedTime(int idOrder) {
        String query = "select TimeReceived from Order where IdOrd = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idOrder);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(rs.getDate(1));
                return calendar;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public int getBuyer(int idOrder) {
        String query = "select IdBuy from Order where IdOrd = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idOrder);
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
    public int getLocation(int idOrder) {
        String query = "select Location from Order where IdOrd = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idOrder);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public static void main(String[] args) {
        da190101_OrderOperationsImpl ordOp = new da190101_OrderOperationsImpl();
        da190101_BuyerOperationsImpl buyOp = new da190101_BuyerOperationsImpl();
        da190101_ShopOperationsImpl shopOp = new da190101_ShopOperationsImpl();
        //buyOp.createOrder(1);
        //ordOp.addArticle(1, 1, 3);
        //ordOp.addArticle(1, 3, 5);
        shopOp.setDiscount(1, 10);
        shopOp.setDiscount(2, 20);
        System.out.println(ordOp.getFinalPrice(1));
    }
}
