package rs.etf.sab.student;

import rs.etf.sab.operations.*;
import org.junit.Test;
import rs.etf.sab.tests.TestHandler;
import rs.etf.sab.tests.TestRunner;

import java.util.Calendar;

public class StudentMain {

    public static void main(String[] args) {

        ArticleOperations articleOperations = new da190101_ArticleOperationsImpl(); // Change this for your implementation (points will be negative if interfaces are not implemented).
        BuyerOperations buyerOperations = new da190101_BuyerOperationsImpl();
        CityOperations cityOperations = new da190101_CityOperationsImpl();
        GeneralOperations generalOperations = new da190101_GeneralOperationsImpl();
        OrderOperations orderOperations = new da190101_OrderOperationsImpl();
        ShopOperations shopOperations = new da190101_ShopOperationsImpl();
        TransactionOperations transactionOperations = new da190101_TransactionOperationsImpl();

        TestHandler.createInstance(
                articleOperations,
                buyerOperations,
                cityOperations,
                generalOperations,
                orderOperations,
                shopOperations,
                transactionOperations
        );

        TestRunner.runTests();
    }
}
