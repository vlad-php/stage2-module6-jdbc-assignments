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

    private interface StatementFunction<S, R> {
        R apply(S statement) throws SQLException;
    }

    private final CustomDataSource dataSource = CustomDataSource.getInstance();

    private Connection connection = null;
    private PreparedStatement ps = null;
    private Statement st = null;

    private static final String CREATE_USER_SQL = """
            INSERT INTO myusers(
            firstname, lastname, age)
            VALUES (?, ?, ?);
            """;
    private static final String UPDATE_USER_SQL = """
            UPDATE myusers
            SET firstname=?, lastname=?, age=?
            WHERE id = ?
            """;
    private static final String DELETE_USER = """
            DELETE FROM public.myusers
            WHERE id = ?
            """;
    private static final String FIND_USER_BY_ID_SQL = """
            SELECT id, firstname, lastname, age FROM myusers
            WHERE id = ?
            """;
    private static final String FIND_USER_BY_NAME_SQL = """
            SELECT id, firstname, lastname, age FROM myusers
            WHERE firstname LIKE CONCAT('%', ?, '%')
            """;
    private static final String FIND_ALL_USER_SQL = """
            SELECT id, firstname, lastname, age FROM myusers
            """;

    private <T> T query(String sqlForStatement,
                        StatementFunction<PreparedStatement, T> statementToResult,
                        int... additionalConstants) {

        try (var conn = dataSource.getConnection();
             var statement = conn.prepareStatement(sqlForStatement, additionalConstants)) {
            return statementToResult.apply(statement);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public Long createUser(User user) {

        return query(
                CREATE_USER_SQL,
                statement -> {
                    statement.setString(1, user.getFirstName());
                    statement.setString(2, user.getLastName());
                    statement.setInt(3, user.getAge());
                    statement.execute();
                    ResultSet generatedKeys = statement.getGeneratedKeys();
                    return generatedKeys.next() ? generatedKeys.getLong(1) : null;
                },
                Statement.RETURN_GENERATED_KEYS
        );

    }

    public User findUserById(Long userId) {

        return query(
                FIND_USER_BY_ID_SQL,
                statement -> {
                    statement.setLong(1, userId);
                    ResultSet resultSet = statement.executeQuery();
                    return resultSet.next() ? map(resultSet) : null;
                }
        );

    }

    public User findUserByName(String userName) {

        return query(
                FIND_USER_BY_NAME_SQL,
                statement -> {
                    statement.setString(1, userName);
                    ResultSet resultSet = statement.executeQuery();
                    return resultSet.next() ? map(resultSet) : null;
                }
        );

    }

    public List<User> findAllUser() {

        return query(
                FIND_ALL_USER_SQL,
                statement -> {
                    List<User> users = new ArrayList<>();
                    ResultSet resultSet = statement.executeQuery();
                    while (resultSet.next()) {
                        users.add(map(resultSet));
                    }
                    return users;
                }
        );

    }

    public User updateUser(User user) {

        return query(
                UPDATE_USER_SQL,
                statement -> {
                    statement.setString(1, user.getFirstName());
                    statement.setString(2, user.getLastName());
                    statement.setInt(3, user.getAge());
                    statement.setLong(4, user.getId());
                    return statement.executeUpdate() != 0 ? findUserById(user.getId()) : null;
                }
        );

    }

    public void deleteUser(Long userId) {

        query(
                DELETE_USER,
                statement -> {
                    statement.setLong(1, userId);
                    statement.executeUpdate();
                    return null;
                }
        );

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