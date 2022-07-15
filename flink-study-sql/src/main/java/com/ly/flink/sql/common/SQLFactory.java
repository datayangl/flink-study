package com.ly.flink.sql.common;

public class SQLFactory {
    public static void main(String[] args) {
        System.out.println(clickDDL);
        System.out.println(orderDDL);

        System.out.println(userDDL);
    }
    public final static String clickDDL = "CREATE TABLE click (\n" +
            " userid int,\n" +
            " clickid int,\n" +
            " item string,\n" +
            " proctime AS PROCTIME()\n" +
            ") WITH (\n" +
            " 'connector' = 'datagen',\n" +
            " 'rows-per-second'='100',\n" +
            " 'fields.userid.kind'='random',\n" +
            " 'fields.userid.min'='1',\n" +
            " 'fields.userid.max'='100',\n" +
            " 'fields.clickid.kind'='random',\n" +
            " 'fields.clickid.min'='1',\n" +
            " 'fields.clickid.max'='100',\n" +
            " 'fields.item.length'='1'" +
            ")";

    public final static String orderDDL = "CREATE TABLE orders (\n" +
            " orderid int,\n" +
            " userid int,\n" +
            " item string,\n" +
            " proctime AS PROCTIME()\n" +
            ") WITH (\n" +
            " 'connector' = 'datagen',\n" +
            " 'rows-per-second'='100',\n" +
            " 'fields.orderid.kind'='random',\n" +
            " 'fields.orderid.min'='1',\n" +
            " 'fields.orderid.max'='100',\n" +
            " 'fields.userid.kind'='random',\n" +
            " 'fields.userid.min'='1',\n" +
            " 'fields.userid.max'='100',\n" +
            " 'fields.item.length'='1'" +
            ")";

    public final static String userDDL = "CREATE TABLE users (\n" +
            "  id int,\n" +
            "  name STRING,\n" +
            "  PRIMARY KEY (id) NOT ENFORCED\n" +
            ") WITH (\n" +
            "   'connector' = 'jdbc',\n" +
            "   'url' = 'jdbc:mysql://localhost:3306/test',\n" +
            "   'table-name' = 'user',\n" +
            "   'username' = 'root',\n" +
            "   'password' = 'root'\n" +
            ")";
}
