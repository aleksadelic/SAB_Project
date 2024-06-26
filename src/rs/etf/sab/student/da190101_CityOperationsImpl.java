package rs.etf.sab.student;

import rs.etf.sab.operations.CityOperations;

import java.sql.*;
import java.util.*;

public class da190101_CityOperationsImpl implements CityOperations {

    static da190101_CityOperationsImpl CITY_OPERATIONS = new da190101_CityOperationsImpl();

    class Wrapper {
        LinkedList<Integer> path;
        int cost;
        int length;

        Wrapper(LinkedList<Integer> path, int cost) {
            this.path = path;
            this.cost = cost;
            this.length = path.size();
        }
    }

    class WrapperComparator implements Comparator<Wrapper> {

        public int compare(Wrapper w1, Wrapper w2) {
            if (w1.cost == w2.cost) {
                return w1.length - w2.length;
            } else {
                return w1.cost - w2.cost;
            }
        }
    }

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
            ps.executeUpdate();
            ps.setInt(1, idCity2);
            ps.setInt(2, idCity1);
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
        String query = "select IdCity2 from IsConnected where IdCity1 = ?";
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

    public int findNearestCityWithShop(int myCity) {
        int numOfCities = getNumberOfCities();
        int offset = calculateOffset();

        int[][] distances = initializeCostsMatrix(numOfCities, offset);

        PriorityQueue<Wrapper> pq = new PriorityQueue<>(new WrapperComparator());
        LinkedList<Integer> path = new LinkedList<>();
        path.add(myCity - offset);
        pq.add(new Wrapper(path, 0));

        while (pq.size() > 0) {
            Wrapper node = pq.poll();
            if (checkIfShopExistsInCity(node.path.getLast() + offset)) {
                System.out.print("Put: ");
                for (int city: node.path) {
                    System.out.print((city + offset) + " ");
                }
                System.out.println("Cena " + node.cost);
                return node.path.getLast() + offset;
            }

            if (node.path.size() == numOfCities) {
                int distance = distances[myCity - offset][node.path.getLast()];
                LinkedList<Integer> list = (LinkedList<Integer>) node.path.clone();
                list.add(myCity - offset);
                pq.add(new Wrapper(list,node.cost + distance));
            } else {
                for (int i = 0; i < numOfCities; i++) {
                    boolean inPath = false;
                    for (int city: node.path) {
                        if (city == i) {
                            inPath = true;
                            break;
                        }
                    }
                    if (inPath) {
                        continue;
                    }

                    int distance = distances[i][node.path.getLast()];
                    LinkedList<Integer> list = (LinkedList<Integer>) node.path.clone();
                    list.add(i);
                    pq.add(new Wrapper(list,node.cost + distance));
                }
            }
        }

        return -1;
    }

    public LinkedList<Integer> findShortestPath(int src, int dst) {
        int numOfCities = getNumberOfCities();
        int offset = calculateOffset();

        int[][] distances = initializeCostsMatrix(numOfCities, offset);

        PriorityQueue<Wrapper> pq = new PriorityQueue<>(new WrapperComparator());
        LinkedList<Integer> path = new LinkedList<>();
        path.add(src - offset);
        pq.add(new Wrapper(path, 0));

        while (pq.size() > 0) {
            Wrapper node = pq.poll();
            if (node.path.getLast() + offset == dst) {
                System.out.print("Put: ");
                for (int city: node.path) {
                    System.out.print((city + offset) + " ");
                }
                System.out.println("Cena " + node.cost);
                // last element is cost
                node.path.add(node.cost);
                return node.path;
            }

            if (node.path.size() == numOfCities) {
                int distance = distances[src - offset][node.path.getLast()];
                LinkedList<Integer> list = (LinkedList<Integer>) node.path.clone();
                list.add(src - offset);
                pq.add(new Wrapper(list,node.cost + distance));
            } else {
                for (int i = 0; i < numOfCities; i++) {
                    boolean inPath = false;
                    for (int city: node.path) {
                        if (city == i) {
                            inPath = true;
                            break;
                        }
                    }
                    if (inPath) {
                        continue;
                    }

                    int distance = distances[i][node.path.getLast()];
                    LinkedList<Integer> list = (LinkedList<Integer>) node.path.clone();
                    list.add(i);
                    pq.add(new Wrapper(list,node.cost + distance));
                }
            }
        }

        return null;
    }

    private int[][] initializeCostsMatrix(int numOfCities, int offset) {

        int[][] distances = new int[numOfCities][numOfCities];

        for (int i = 0; i < numOfCities; i++) {
            for (int j = 0; j < numOfCities; j++) {
                distances[i][j] = Integer.MAX_VALUE;
            }
        }

        String query = "select * from IsConnected";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                int city1 = rs.getInt(1) - offset;
                int city2 = rs.getInt(2) - offset;
                int distance = rs.getInt(3);
                distances[city1][city2] = distance;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return distances;
    }

    public int getNumberOfCities() {
        int numOfCities = 0;
        String query = "select count(*) from City";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                numOfCities = rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return numOfCities;
    }

    public int calculateOffset() {
        int offset = 0;
        String query = "select min(IdCity) from City";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                offset = rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return offset;
    }

    public boolean checkIfShopExistsInCity(int idCity) {
        String query = "select * from Shop where IdCity = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, idCity);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static void main(String[] args) {
        da190101_CityOperationsImpl obj = new da190101_CityOperationsImpl();

        obj.connectCities(1, 2, 8);
        obj.connectCities(1, 3, 2);
        obj.connectCities(2, 4, 10);
        obj.connectCities(3, 4, 15);
        obj.connectCities(4, 5, 3);
        obj.connectCities(4, 6, 3);
        obj.connectCities(5, 7, 2);
        obj.connectCities(6, 7, 1);

        System.out.println(obj.findNearestCityWithShop(1));
        obj.findShortestPath(7, 4);
    }
}
