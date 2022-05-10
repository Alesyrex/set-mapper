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
    public static final String ID = "id";
    public static final String MANAGER = "manager";
    public static final String FIRSTNAME = "firstname";
    public static final String MIDDLENAME = "middlename";
    public static final String LASTNAME = "lastname";
    public static final String POSITION = "position";
    public static final String HIREDATE = "hiredate";
    public static final String SALARY = "salary";
    public static final String EXCEPTION_LOG_FORMAT = "Exception: ";

    private static final Logger LOGGER = Logger.getLogger(EmployeeSetMapper.class.getName());

    public Employee getEmployee (ResultSet resultSet) throws SQLException {
        Employee manager = null;
        BigDecimal managerId = resultSet.getBigDecimal(MANAGER);
        if (!resultSet.wasNull()) {
            manager = getManager(managerId, resultSet);
        }
        BigInteger id = resultSet.getBigDecimal(ID).toBigInteger();
        FullName fullName = new FullName(resultSet.getString(FIRSTNAME),
                resultSet.getString(LASTNAME), resultSet.getString(MIDDLENAME));
        Position position = Position.valueOf(resultSet.getString(POSITION));
        LocalDate hired = resultSet.getDate(HIREDATE).toLocalDate();
        return new Employee(id, fullName, position,
                hired, resultSet.getBigDecimal(SALARY), manager);
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
    public Set<Employee> mapSet(ResultSet resultSet) {
        Set<Employee> result = new HashSet<>();
        try {
            while (resultSet.next()) {
                result.add(getEmployee(resultSet));
            }
        } catch (SQLException sqlEx) {
            LOGGER.log(Level.SEVERE, EXCEPTION_LOG_FORMAT, sqlEx);
        }
        return result;
    }
}
