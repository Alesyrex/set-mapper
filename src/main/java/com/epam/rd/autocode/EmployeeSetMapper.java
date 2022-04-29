package com.epam.rd.autocode;

import com.epam.rd.autocode.domain.Employee;
import com.epam.rd.autocode.domain.FullName;
import com.epam.rd.autocode.domain.Position;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class EmployeeSetMapper implements SetMapper<Set<Employee>> {
    private static final Logger LOGGER = Logger.getLogger(EmployeeSetMapper.class.getName());
    private static final String ID = "id";

    public Employee getEmployee (ResultSet resultSet) throws SQLException {
        Employee manager = null;
        BigDecimal idManager = resultSet.getBigDecimal("manager");
        if (!resultSet.wasNull()) {
            manager = getManager(idManager, resultSet);
        }
        BigInteger id = resultSet.getBigDecimal(ID).toBigInteger();
        FullName fullName = new FullName(resultSet.getString("firstname"),
                resultSet.getString("lastname"), resultSet.getString("middlename"));
        Position position = Position.valueOf(resultSet.getString("position"));
        LocalDate hired = resultSet.getDate("hiredate").toLocalDate();
        return new Employee(id, fullName, position,
                hired, resultSet.getBigDecimal("salary"), manager);
    }

    public Employee getManager (BigDecimal idManager, ResultSet res) throws SQLException {
        Employee manager = null;
        int currentRow = res.getRow();
        res.beforeFirst();
        while (res.next()) {
            if (res.getBigDecimal(ID).equals(idManager)) {
                manager = getEmployee(res);
                break;
            }
        }
        res.absolute(currentRow);
        return manager;
    }

    @Override
    public Set<Employee> mapSet(ResultSet resultSet) throws SQLException {
        Set<Employee> result = new HashSet<>();
        try {
            while (resultSet.next()) {
                result.add(getEmployee(resultSet));
            }
        } catch (SQLException sqlEx) {
            LOGGER.log(Level.SEVERE, "Exception: ", sqlEx);
            throw sqlEx;
        }
        return result;
    }
}
