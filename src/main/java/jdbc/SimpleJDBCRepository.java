package jdbc;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class SimpleJDBCRepository {
    private final CustomDataSource dataSource = CustomDataSource.getInstance();

    private Connection connection = null;
    private PreparedStatement ps = null;
    private Statement st = null;

    private static final String createUserSQL = """
            INSERT INTO myusers(
            firstname, lastname, age)
            VALUES (?, ?, ?);
            """;
    private static final String updateUserSQL ="""
            UPDATE myusers
            SET firstname=?, lastname=?, age=?
            WHERE id = ?
            """;
    private static final String deleteUser = """
            DELETE FROM public.myusers
            WHERE id = ?
            """;
    private static final String findUserByIdSQL = """
            SELECT id, firstname, lastname, age FROM myusers
            WHERE id = ?
            """;
    private static final String findUserByNameSQL = """
            SELECT id, firstname, lastname, age FROM myusers
            WHERE firstname LIKE CONCAT('%', ?, '%')
            """;
    private static final String findAllUserSQL = """
            SELECT id, firstname, lastname, age FROM myusers
            """;

    public Long createUser(User user) {
        try {
            Connection connection = dataSource.getConnection();
            ps = connection.prepareStatement(createUserSQL);
            ps.setString(1,user.getFirstName());
            ps.setString(2,user.getLastName());
            ps.setInt(3,user.getAge());
            ps.execute();
            ResultSet generatedKeys = ps.getGeneratedKeys();
            return generatedKeys.next() ? generatedKeys.getLong(1) : null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public User findUserById(Long userId) {
        try {
            Connection connection = dataSource.getConnection();
            ps = connection.prepareStatement(findUserByIdSQL);
            ps.setLong(1,userId);
            ResultSet resultSet = ps.executeQuery();
            return resultSet.next() ? map(resultSet) : null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public User findUserByName(String userName) {
        try {
            Connection connection = dataSource.getConnection();
            ps = connection.prepareStatement(findUserByNameSQL);
            ps.setString(1,userName);
            ResultSet resultSet = ps.executeQuery();
            return resultSet.next() ? map(resultSet) : null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<User> findAllUser() {
        try {
            Connection connection = dataSource.getConnection();
            ps = connection.prepareStatement(findAllUserSQL);
            List<User> users = new ArrayList<>();
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                users.add(map(resultSet));
            }
            return users;
        }catch (SQLException e){
            throw new RuntimeException(e);
        }
    }

    public User updateUser(User user) {
        try {
            Connection connection = dataSource.getConnection();
            ps = connection.prepareStatement(updateUserSQL);
            ps.setString(1, user.getFirstName());
            ps.setString(2, user.getLastName());
            ps.setInt(3, user.getAge());
            ps.setLong(4, user.getId());
            return ps.executeUpdate() != 0 ? findUserById(user.getId()) : null;
        }catch (SQLException e){
            throw new RuntimeException(e);
        }
    }

    private void deleteUser(Long userId) {
        try {
            Connection connection = dataSource.getConnection();
            ps = connection.prepareStatement(deleteUser);
            ps.setLong(1, userId);
            ps.executeUpdate();
        }catch (SQLException e){
            throw new RuntimeException(e);
        }
    }
    private User map(ResultSet rs) throws SQLException {

        return User.builder()
                .id(rs.getLong("id"))
                .firstName(rs.getString("firstname"))
                .lastName(rs.getString("lastname"))
                .age(rs.getInt("age"))
                .build();
    }
}
