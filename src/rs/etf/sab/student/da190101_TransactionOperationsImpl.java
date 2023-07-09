package rs.etf.sab.student;

import rs.etf.sab.operations.TransactionOperations;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class da190101_TransactionOperationsImpl implements TransactionOperations {

    static da190101_TransactionOperationsImpl TRANSACTION_OPERATIONS = new da190101_TransactionOperationsImpl();

    Connection connection = DB.getInstance().getConnection();

    @Override
    public BigDecimal getBuyerTransactionsAmmount(int idBuyer) {
        String query = "select sum(Ammount) from [Transaction] join BuyerTransaction on " +
                "[Transaction].IdTra = BuyerTransaction.IdTra where IdBuy = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idBuyer);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return new BigDecimal(rs.getDouble(1)).setScale(3);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public BigDecimal getShopTransactionsAmmount(int idShop) {
        String query = "select sum(Ammount) from [Transaction] join ShopTransaction on " +
                "[Transaction].IdTra = ShopTransaction.IdTra where IdShop = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idShop);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return new BigDecimal(rs.getDouble(1)).setScale(3);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<Integer> getTransationsForBuyer(int idBuyer) {
        List<Integer> list = new ArrayList<>();
        String query = "select IdTra from BuyerTransaction where IdBuy = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idBuyer);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                list.add(rs.getInt(1));
            }
            if (list.size() != 0) return list;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public int getTransactionForBuyersOrder(int idOrder) {
        String query = "select [Transaction].IdTra from [Transaction] join BuyerTransaction on " +
                "[Transaction].IdTra = BuyerTransaction.IdTra where IdOrd = ?";
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
    public int getTransactionForShopAndOrder(int idOrder, int idShop) {
        String query = "select [Transaction].IdTra from [Transaction] join ShopTransaction on " +
                "[Transaction].IdTra = ShopTransaction.IdTra where IdOrd = ? and IdShop = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idOrder);
            ps.setInt(2, idShop);
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
    public List<Integer> getTransationsForShop(int idShop) {
        List<Integer> list = new ArrayList<>();
        String query = "select IdTra from ShopTransaction where IdShop = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idShop);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                list.add(rs.getInt(1));
            }
            if (list.size() != 0) return list;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Calendar getTimeOfExecution(int idTransaction) {
        String query = "select ExecutionTime from [Transaction] where IdTra = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idTransaction);
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
    public BigDecimal getAmmountThatBuyerPayedForOrder(int idOrder) {
        String query = "select Ammount from [Transaction] join BuyerTransaction on " +
                "[Transaction].IdTra = BuyerTransaction.IdTra where IdOrd = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idOrder);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return new BigDecimal(rs.getDouble(1)).setScale(3);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public BigDecimal getAmmountThatShopRecievedForOrder(int idShop, int idOrder) {
        String query = "select Ammount from [Transaction] join ShopTransaction on " +
                "[Transaction].IdTra = ShopTransaction.IdTra where IdShop = ? and IdOrd = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idShop);
            ps.setInt(2, idOrder);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return new BigDecimal(rs.getDouble(1)).setScale(3);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public BigDecimal getTransactionAmount(int idTransaction) {
        String query = "select Ammount from [Transaction] where IdTra = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idTransaction);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return new BigDecimal(rs.getDouble(1)).setScale(3);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public BigDecimal getSystemProfit() {
        String query = "select sum(Ammount) from [Transaction] join SystemTransaction on " +
                "[Transaction].IdTra = SystemTransaction.IdTra";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return new BigDecimal(rs.getDouble(1)).setScale(3);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public int createTransactions(int idOrder, Calendar receivedTime) {
        BigDecimal finalPrice = da190101_OrderOperationsImpl.ORDER_OPERATIONS.getFinalPrice(idOrder);

        if (createShopTransaction(idOrder, receivedTime) == -1) {
            System.out.println("Shop transaction failed");
            return -1;
        }

        if (createSystemTransaction(idOrder, finalPrice, receivedTime) == -1) {
            System.out.println("System transaction failed");
            return -1;
        }

        String query = "update [Order] set Status = 'arrived', TimeReceived = ? where IdOrd = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(2, idOrder);
            ps.setDate(1, new java.sql.Date(receivedTime.getTime().getTime()));
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            return -1;
        }

        return 1;
    }

    public int createBuyerTransaction(int idOrder, BigDecimal price) {
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

        query = "insert into [Transaction] (Ammount, IdOrd, ExecutionTime) values (?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {
            ps.setDouble(1, price.doubleValue());
            ps.setInt(2, idOrder);
            ps.setDate(3, new java.sql.Date(da190101_GeneralOperationsImpl.
                    GENERAL_OPERATIONS.getCurrentTime().getTime().getTime()));
            ps.executeUpdate();
            ResultSet rs = ps.getGeneratedKeys();
            if (rs.next()) {
                String query1 = "insert into BuyerTransaction (IdTra, IdBuy) values (?, ?)";
                PreparedStatement ps1 = connection.prepareStatement(query1);
                ps1.setInt(1, rs.getInt(1));
                ps1.setInt(2, idBuyer);
                ps1.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return -1;
        }

        // update articles quantity
        query = "select Article.IdArt, Item.Quantity, Article.Quantity from [Order] join Item on [Order].IdOrd = Item.IdOrd " +
                "join Article on Item.IdArt = Article.IdArt where [Order].IdOrd = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idOrder);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String queryUpdate = "update Article set Quantity = ? where IdArt = ?";
                PreparedStatement psUpdate = connection.prepareStatement(queryUpdate);
                psUpdate.setInt(1,rs.getInt(3) - rs.getInt(2));
                psUpdate.setInt(2, rs.getInt(1));
                psUpdate.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return -1;
        }

        return 1;
    }

    private int createShopTransaction(int idOrder, Calendar receivedTime) {
        String query = "select distinct(Shop.IdShop) from Shop join Article on Shop.IdShop = Article.IdShop join" +
                " Item on Item.IdArt = Article.IdArt join [Order] on Item.IdOrd = [Order].IdOrd where [Order].IdOrd = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idOrder);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                double shopProfit = getShopProfit(idOrder, rs.getInt(1)).doubleValue() * 0.95;
                String query1 = "insert into [Transaction] (Ammount, IdOrd, ExecutionTime) values (?, ?, ?)";
                PreparedStatement ps1 = connection.prepareStatement(query1, Statement.RETURN_GENERATED_KEYS);
                ps1.setDouble(1, shopProfit);
                ps1.setInt(2, idOrder);
                ps1.setDate(3, new java.sql.Date(receivedTime.getTime().getTime()));
                ps1.executeUpdate();
                ResultSet rs1 = ps1.getGeneratedKeys();
                if (rs1.next()) {
                    String query2 = "insert into ShopTransaction (IdTra, IdShop) values (?, ?)";
                    PreparedStatement ps2 = connection.prepareStatement(query2);
                    ps2.setInt(1, rs1.getInt(1));
                    ps2.setInt(2, rs.getInt(1));
                    ps2.executeUpdate();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return -1;
        }

        return 1;
    }

    private int createSystemTransaction(int idOrder, BigDecimal price, Calendar receivedTime) {
        String query = "insert into [Transaction] (Ammount, IdOrd, ExecutionTime) values (?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {
            ps.setDouble(1, price.doubleValue() * 0.05);
            ps.setInt(2, idOrder);
            ps.setDate(3, new java.sql.Date(receivedTime.getTime().getTime()));
            ps.executeUpdate();
            ResultSet rs = ps.getGeneratedKeys();
            if (rs.next()) {
                String query1 = "insert into SystemTransaction (IdTra) values (?)";
                PreparedStatement ps1 = connection.prepareStatement(query1);
                ps1.setInt(1,rs.getInt(1));
                return ps1.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public BigDecimal getShopProfit(int idOrder, int idShop) {
        String query = "select Item.IdArt, Item.Quantity, Price from Item Join Article on Item.IdArt = Article.IdArt where IdItem = ? and Article.IdShop = ?";
        double sum = 0;
        List<Integer> items = da190101_OrderOperationsImpl.ORDER_OPERATIONS.getItems(idOrder);
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
        List<Integer> items = da190101_OrderOperationsImpl.ORDER_OPERATIONS.getItems(idOrder);
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

    public static void main(String[] args) {
        da190101_TransactionOperationsImpl traObj = new da190101_TransactionOperationsImpl();
        System.out.println(traObj.getAmmountThatBuyerPayedForOrder(1).doubleValue());
        System.out.println(traObj.getAmmountThatShopRecievedForOrder(1, 1).doubleValue());
        System.out.println(traObj.getBuyerTransactionsAmmount(1).doubleValue());
        System.out.println(traObj.getSystemProfit().doubleValue());
        System.out.println(traObj.getTransactionForBuyersOrder(1));
        System.out.println(traObj.getTransactionForShopAndOrder(1, 2));

        System.out.println("Lista transakcija kupca 1: ");
        List<Integer> list = traObj.getTransationsForBuyer(1);
        for (int tra: list) {
            System.out.println(tra);
        }

        System.out.println("Lista transakcija prodavnice 1: ");
        list = traObj.getTransationsForShop(1);
        for (int tra: list) {
            System.out.println(tra);
        }

        System.out.println(traObj.getTransactionAmount(1));
    }
}
