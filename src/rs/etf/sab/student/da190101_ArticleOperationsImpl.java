package rs.etf.sab.student;

import rs.etf.sab.operations.ArticleOperations;

import java.sql.*;

public class da190101_ArticleOperationsImpl implements ArticleOperations {

    Connection connection = DB.getInstance().getConnection();

    @Override
    public int createArticle(int idShop, String name, int price) {
        String query = "insert into Article (IdShop, Name, Price) values (?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {
            ps.setInt(1, idShop);
            ps.setString(2, name);
            ps.setInt(3, price);
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

    public static void main(String[] args) {
        da190101_ArticleOperationsImpl obj = new da190101_ArticleOperationsImpl();
        obj.createArticle(3, "Bicikl Drugi", 25000);
    }
}
