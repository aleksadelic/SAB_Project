package rs.etf.sab.student;

import rs.etf.sab.operations.TransactionOperations;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class da190101_TransactionOperationsImpl implements TransactionOperations {

    Connection connection = DB.getInstance().getConnection();

    @Override
    public BigDecimal getBuyerTransactionsAmmount(int idBuyer) {
        String query = "select sum(TotalPrice) from BuyerTransaction where IdBuy = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idBuyer);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return new BigDecimal(rs.getDouble(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public BigDecimal getShopTransactionsAmmount(int idShop) {
        String query = "select sum(Profit) from ShopTransaction where IdShop = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idShop);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return new BigDecimal(rs.getDouble(1));
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
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
        return list;
    }

    @Override
    public int getTransactionForBuyersOrder(int idOrder) {
        String query = "select IdTra from BuyerTransaction where IdOrd = ?";
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
        String query = "select IdTra from ShopTransaction where IdOrd = ? and IdShop = ?";
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
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
        return list;
    }

    @Override
    public Calendar getTimeOfExecution(int i) {
        return null;
    }

    @Override
    public BigDecimal getAmmountThatBuyerPayedForOrder(int idOrder) {
        String query = "select TotalPrice from BuyerTransaction where IdOrd = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idOrder);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return new BigDecimal(rs.getDouble(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public BigDecimal getAmmountThatShopRecievedForOrder(int idShop, int idOrder) {
        String query = "select Profit from ShopTransaction where IdShop = ? and IdOrd = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idShop);
            ps.setInt(2, idOrder);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return new BigDecimal(rs.getDouble(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public BigDecimal getTransactionAmount(int i) {
        return null;
    }

    @Override
    public BigDecimal getSystemProfit() {
        String query = "select sum(Profit) from SystemTransaction";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return new BigDecimal(rs.getDouble(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        da190101_TransactionOperationsImpl traObj = new da190101_TransactionOperationsImpl();
        System.out.println(traObj.getAmmountThatBuyerPayedForOrder(40).doubleValue());
        System.out.println(traObj.getAmmountThatShopRecievedForOrder(116, 40).doubleValue());
        System.out.println(traObj.getBuyerTransactionsAmmount(62).doubleValue());
        System.out.println(traObj.getSystemProfit().doubleValue());
        System.out.println(traObj.getTransactionForBuyersOrder(40));
        System.out.println(traObj.getTransactionForShopAndOrder(46, 125));

        System.out.println("Lista transakcija kupca 62: ");
        List<Integer> list = traObj.getTransationsForBuyer(62);
        for (int tra: list) {
            System.out.println(tra);
        }

        System.out.println("Lista transakcija prodavnice 116: ");
        list = traObj.getTransationsForShop(116);
        for (int tra: list) {
            System.out.println(tra);
        }
    }
}
