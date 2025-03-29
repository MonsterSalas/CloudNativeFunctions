package com.function;

public class Main {
    public static void main(String[] args) {
        System.out.println("Probando conexión a la base de datos...");
        try {
            boolean isConnected = OracleDBConnection.testConnection();
            System.out.println("Conexión a la base de datos: " + (isConnected ? "Exitosa" : "Fallida"));
        } catch (Exception e) {
            System.err.println("Error al probar la conexión: " + e.getMessage());
        }
    }
}