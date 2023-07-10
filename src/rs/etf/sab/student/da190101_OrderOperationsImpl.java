package rs.etf.sab.student;

import rs.etf.sab.operations.GeneralOperations;
import rs.etf.sab.operations.OrderOperations;

import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;

public class da190101_OrderOperationsImpl implements OrderOperations {

    Connection connection = DB.getInstance().getConnection();

    static da190101_OrderOperationsImpl ORDER_OPERATIONS = new da190101_OrderOperationsImpl();

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
        if (!checkIfOrderIsPossible(idOrder, finalPrice)) {
            System.out.println("Order is not possible");
            return -1;
        }

        da190101_TransactionOperationsImpl.TRANSACTION_OPERATIONS.createBuyerTransaction(idOrder, finalPrice);

        String query = "update [Order] set Status = 'sent', TimeSent = ? where IdOrd = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setDate(1, new java.sql.Date(da190101_GeneralOperationsImpl.
                    GENERAL_OPERATIONS.getCurrentTime().getTime().getTime()));
            ps.setInt(2, idOrder);
            ps.executeUpdate();
            return fillOrderMap(idOrder);
        } catch (SQLException e) {
            e.printStackTrace();
            return -1;
        }
    }

    private int fillOrderMap(int idOrder) {
        int idBuyer = getBuyer(idOrder);
        int buyerCity = da190101_BuyerOperationsImpl.BUYER_OPERATIONS.getCity(idBuyer);
        int nearestCity = da190101_CityOperationsImpl.CITY_OPERATIONS.findNearestCityWithShop(buyerCity);
        int offset = da190101_CityOperationsImpl.CITY_OPERATIONS.calculateOffset();

        // time to get all orders to this city
        ArrayList<Integer> list = new ArrayList<>();
        String query = "select distinct(IdCity) from [Order] join Item on [Order].IdOrd = Item.IdOrd " +
                "join Article on Item.IdArt = Article.IdArt join Shop on Article.IdShop = Shop.IdShop where " +
                "[Order].IdOrd = ?";

        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idOrder);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                list.add(rs.getInt(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return -1;
        }

        int max = 0;
        for (int city: list) {
            LinkedList<Integer> path = da190101_CityOperationsImpl.CITY_OPERATIONS.findShortestPath(city, nearestCity);
            if (path != null) {
                int cost = path.getLast();
                if (cost > max) {
                    max = cost;
                }
            }
        }

        LinkedList<Integer> path = da190101_CityOperationsImpl.CITY_OPERATIONS.findShortestPath(nearestCity, buyerCity);
        if (path == null) {
            return -1;
        }
        // last element is path cost
        path.removeLast();

        int[][] locations = new int[2][path.size() + 1];
        locations[0][0] = 0;
        locations[1][0] = nearestCity;
        locations[0][1] = max;
        locations[1][1] = nearestCity;
        int ind = 2;

        int i = 0;
        int j = i + 1;
        while (j < path.size()) {
            query = "select Distance from IsConnected where IdCity1 = ? and IdCity2 = ?";
            try (PreparedStatement ps = connection.prepareStatement(query)) {
                ps.setInt(1, path.get(i) + offset);
                ps.setInt(2, path.get(j) + offset);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    locations[0][ind] = locations[0][ind - 1] + rs.getInt(1);
                    locations[1][ind] = path.get(j) + offset;
                    ind++;
                }
            } catch (SQLException e) {
                e.printStackTrace();
                return -1;
            }

            i++;
            j++;
        }

        System.out.println("Lokacije u trenucima: ");
        for (i = 0; i < locations[0].length; i++) {
            System.out.print(locations[0][i] + " ");
            System.out.println(locations[1][i]);
        }

        query = "update [Order] set Location = ? where IdOrd = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, nearestCity);
            ps.setInt(2, idOrder);
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            return -1;
        }

        da190101_GeneralOperationsImpl.GENERAL_OPERATIONS.orderMap.put(idOrder, locations);
        return 1;
    }

    private boolean checkIfOrderIsPossible(int idOrder, BigDecimal price) {
        // check buyer credit
        String query = "select Credit from Buyer join [Order] on Buyer.IdBuy = [Order].IdBuy where [Order].IdOrd = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idOrder);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                if (price.doubleValue() > rs.getDouble(1)) {
                    return false;
                }
            } else {
                return false;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }

        // check if articles are available
        query = "select Item.Quantity, Article.Quantity from [Order] join Item on [Order].IdOrd = Item.IdOrd " +
                "join Article on Item.IdArt = Article.IdArt where [Order].IdOrd = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idOrder);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                if (rs.getInt(1) > rs.getInt(2)) {
                    return false;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public BigDecimal getFinalPrice(int idOrder) {
        String query = "{ call SP_FINAL_PRICE (?, ?, ?) }";
        try (CallableStatement cs = connection.prepareCall(query)) {
            cs.setInt(1, idOrder);
            cs.setDate(2, new java.sql.Date(da190101_GeneralOperationsImpl.
                    GENERAL_OPERATIONS.getCurrentTime().getTime().getTime()));
            cs.registerOutParameter(3, Types.DECIMAL);
            cs.execute();
            return new BigDecimal(cs.getDouble(3)).setScale(3);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public BigDecimal getFinalPriceWithoutAdditionalDiscount(int idOrder) {
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
        return new BigDecimal(sum).subtract(getDiscountSum(idOrder)).setScale(3);
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
        return new BigDecimal(sum).setScale(3);
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
                if (rs.getDate(1) == null) return null;
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
                if (rs.getDate(1) == null) return null;
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

        Calendar initialTime = Calendar.getInstance();
        initialTime.clear();
        initialTime.set(2018, 1, 15);
        da190101_GeneralOperationsImpl.GENERAL_OPERATIONS.setInitialTime(initialTime);

        shopOp.setDiscount(1, 10);
        shopOp.setDiscount(3, 20);

        int idOrder = buyOp.createOrder(1);
        ordOp.addArticle(idOrder, 1, 2);
        ordOp.addArticle(idOrder, 3, 3);
        ordOp.addArticle(idOrder, 7, 1);

        ordOp.completeOrder(idOrder);

        System.out.println("Dan 0: " + ordOp.getLocation(idOrder));

        da190101_GeneralOperationsImpl.GENERAL_OPERATIONS.time(3);

        System.out.println("Dan 3: " + ordOp.getLocation(idOrder));

        da190101_GeneralOperationsImpl.GENERAL_OPERATIONS.time(10);

        System.out.println("Dan 13: " + ordOp.getLocation(idOrder));

        da190101_GeneralOperationsImpl.GENERAL_OPERATIONS.time(7);

        System.out.println("Dan 20: " + ordOp.getLocation(idOrder));

        da190101_GeneralOperationsImpl.GENERAL_OPERATIONS.time(1);

        System.out.println("Dan 21: " + ordOp.getLocation(idOrder));
    }
}
