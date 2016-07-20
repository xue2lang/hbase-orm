package com.flipkart.hbaseobjectmapper.entities;

import com.flipkart.hbaseobjectmapper.HBColumn;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.HBRowKey;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString
@EqualsAndHashCode
public class Employee implements HBRecord<Long> {
    @HBRowKey
    private Long empid;

    @HBColumn(family = "a", column = "name")
    private String empName;

    @Override
    public Long composeRowKey() {
        return empid;
    }

    @Override
    public void parseRowKey(Long rowKey) {
        empid = rowKey;
    }

    public Long getEmpid() {
        return empid;
    }

    public String getEmpName() {
        return empName;
    }

    public Employee() {

    }

    public Employee(Long empid, String empName) {
        this.empid = empid;
        this.empName = empName;
    }
}
