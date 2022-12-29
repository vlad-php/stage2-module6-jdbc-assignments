package jdbc;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
public class SimpleJDBCRepository {

    private Connection connection = new CustomConnector().getConnection(CustomDataSource.getInstance().getUrl(), CustomDataSource.getInstance().getName(), CustomDataSource.getInstance().getPassword());
    private PreparedStatement ps = null;
    private Statement st = null;
    public SimpleJDBCRepository(Connection connection, PreparedStatement ps, Statement st) {
        CustomConnector customConnector = new CustomConnector();
        CustomDataSource customDataSource = CustomDataSource.getInstance();
        this.connection = customConnector.getConnection(customDataSource.getUrl(), customDataSource.getName(), customDataSource.getPassword());
        this.ps = ps;
        try {
            this.st = connection.createStatement();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static final String createUserSQL = """
            INSERT INTO myusers(
            firstname, lastname, age)
            VALUES (?, ?, ?);
            """;
    private static final String updateUserSQL = """
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
            ps = connection.prepareStatement(createUserSQL);
            ps.setString(1, user.getFirstName());
            ps.setString(2, user.getLastName());
            ps.setInt(3, user.getAge());
            ResultSet rs = ps.executeQuery();
            return rs.next() ? rs.getLong(1) : null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public User findUserById(Long userId) {
        try {
            ps = connection.prepareStatement(findUserByIdSQL);
            ps.setInt(1, Math.toIntExact(userId));
            ResultSet rs = ps.executeQuery();
            return rs.next() ? parsUser(rs) : null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public User findUserByName(String userName) {
        try {
            ps = connection.prepareStatement(findUserByNameSQL);
            ps.setString(1, userName);
            ResultSet rs = ps.executeQuery();
            return rs.next() ? parsUser(rs) : null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<User> findAllUser() {
        List<User> users = new ArrayList<>();
        try {
            ps = connection.prepareStatement(findAllUserSQL);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                users.add(parsUser(rs));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return users;
    }

    public User updateUser(User user) {
        try {
            ps = connection.prepareStatement(updateUserSQL);
            ps.setString(1, user.getFirstName());
            ps.setString(2, user.getLastName());
            ps.setInt(3, user.getAge());
            ps.setLong(4, user.getId());
            return ps.executeUpdate() != 0 ? findUserById(user.getId()) : null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteUser(Long userId) {
        try {
            ps = connection.prepareStatement(deleteUser);
            ps.setInt(1, Math.toIntExact(userId));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private User parsUser(ResultSet rs) throws SQLException {
        return User.builder()
                .id(rs.getLong("id"))
                .firstName(rs.getString("firstname"))
                .lastName(rs.getString("lastname"))
                .age(rs.getInt("age"))
                .build();
    }
}