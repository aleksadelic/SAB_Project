package rs.etf.sab.student;

import rs.etf.sab.operations.GeneralOperations;
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
    public int completeOrder(int idOrder) {
        BigDecimal finalPrice = getFinalPrice(idOrder);
        if (createBuyerTransaction(idOrder, finalPrice) == -1) {
            return -1;
        }

        if (createShopTransaction(idOrder) == -1) {
            return -1;
        }

        if (createSystemTransaction(idOrder, finalPrice) == -1) {
            return -1;
        }

        String query = "update [Order] set Status = 'Sent', TimeSent = ? where idOrder = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setDate(1, new java.sql.Date(da190101_GeneralOperationsImpl.
                    GENERAL_OPERATIONS.getCurrentTime().getTime().getTime()));
            ps.setInt(2, idOrder);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    private int createBuyerTransaction(int idOrder, BigDecimal price) {
        // check buyer credit
        String query = "select Buyer.IdBuy, Credit from Buyer join [Order] on Buyer.IdBuy = [Order].IdBuy where [Order].IdOrd = ?";
        int idBuyer = 0;
        double newCredit = 0;
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idOrder);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                idBuyer = rs.getInt(1);
                newCredit = rs.getDouble(2) - price.doubleValue();
                if (newCredit < 0) {
                    return -1;
                }
            } else {
                return -1;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return -1;
        }

        query = "update Buyer set Credit = ? where IdBuy = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(2, idBuyer);
            ps.setDouble(1, newCredit);
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        query = "insert into BuyerTransaction (TotalPrice, IdOrd, IdBuy) values (?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setDouble(1, price.doubleValue());
            ps.setInt(2, idOrder);
            ps.setInt(3, idBuyer);
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            return -1;
        }

        return 1;
    }

    private int createShopTransaction(int idOrder) {
        String query = "select distinct(Shop.IdShop) from Shop join Article on Shop.IdShop = Article.IdShop join" +
                " Item on Item.IdArt = Article.IdArt join [Order] on Item.IdOrd = [Order].IdOrd where [Order].IdOrd = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idOrder);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                double shopProfit = getShopProfit(idOrder, rs.getInt(1)).doubleValue() * 0.95;
                String query1 = "insert into ShopTransaction (Profit, IdOrd, IdShop) values (?, ?, ?)";
                PreparedStatement ps1 = connection.prepareStatement(query1);
                ps1.setDouble(1, shopProfit);
                ps1.setInt(2, idOrder);
                ps1.setInt(3, rs.getInt(1));
                ps1.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return -1;
        }
        return 1;
    }

    private int createSystemTransaction(int idOrder, BigDecimal price) {
        String query = "insert into SystemTransaction (Profit, IdOrd) values (?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setDouble(1, price.doubleValue() * 0.05);
            ps.setInt(2, idOrder);
            return ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
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

    public BigDecimal getShopProfit(int idOrder, int idShop) {
        String query = "select Item.IdArt, Item.Quantity, Price from Item Join Article on Item.IdArt = Article.IdArt where IdItem = ? and Article.IdShop = ?";
        double sum = 0;
        List<Integer> items = getItems(idOrder);
        for (int item: items) {
            try (PreparedStatement ps = connection.prepareStatement(query)) {
                ps.setInt(1, item);
                ps.setInt(2, idShop);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    sum += rs.getInt(2) * rs.getDouble(3);
                }
            } catch (SQLException e) {
                e.printStackTrace();
                return null;
            }
        }
        return new BigDecimal(sum).subtract(getDiscountSumShop(idOrder, idShop));
    }

    public BigDecimal getDiscountSumShop(int idOrder, int idShop) {
        String query = "select Item.IdArt, Item.Quantity, Price, Discount from Item Join Article on Item.IdArt = Article.IdArt" +
                " join Shop on Article.IdShop = Shop.IdShop where IdItem = ? and Shop.IdShop = ?";
        double sum = 0;
        List<Integer> items = getItems(idOrder);
        for (int item: items) {
            try (PreparedStatement ps = connection.prepareStatement(query)) {
                ps.setInt(1, item);
                ps.setInt(2, idShop);
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
        String query = "select Status from [Order] where IdOrd = ?";
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
        String query = "select TimeSent from [Order] where IdOrd = ?";
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
        String query = "select TimeReceived from [Order] where IdOrd = ?";
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
        String query = "select IdBuy from [Order] where IdOrd = ?";
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
        String query = "select Location from [Order] where IdOrd = ?";
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
        //int idOrder = buyOp.createOrder(62);
        int idOrder = 40;
        ordOp.addArticle(idOrder, 74, 3);
        ordOp.addArticle(idOrder, 75, 5);
        /*shopOp.setDiscount(1, 10);
        shopOp.setDiscount(2, 20);*/
        System.out.println(ordOp.getFinalPrice(idOrder));
        ordOp.completeOrder(idOrder);
    }
}
