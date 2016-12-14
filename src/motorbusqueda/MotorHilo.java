/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package motorbusqueda;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URL;
import java.security.SecureRandom;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

public class MotorHilo extends Thread {

    static int cont = 0;
    static PreparedStatement preparada;

    @Override
    public void run() {
        String nombre = nextSessionId() + "";
        Connection conn = getConexion();
        System.out.println("Hilo " + nombre + " iniciado");
        while (true) {
            String enlaceInicial = obtenerEnlace(conn, nombre);
            extraerEnlacesURL(conn, enlaceInicial);
        }
    }

    private static String obtenerEnlace(Connection conn, String nombre) {
        String enlace = "";
        try {
            //Limpia rezagados
            String sql = "UPDATE enlaces SET estado = 'pendiente' WHERE estado = '" + nombre + "'";
            preparada = (PreparedStatement) conn.prepareStatement(sql);
            preparada.executeUpdate();
            //La sentencia mysql
            sql = "UPDATE enlaces SET estado = '" + nombre + "' WHERE estado = 'pendiente' LIMIT 1";
            preparada = (PreparedStatement) conn.prepareStatement(sql);
            preparada.executeUpdate();
            //Selecciona el enlace
            sql = "SELECT * FROM enlaces WHERE estado = '" + nombre + "' LIMIT 1";
            //Crea la sentencia preparada
            preparada = (PreparedStatement) conn.prepareStatement(sql);
            ResultSet res = preparada.executeQuery();
            while (res.next()) {
                enlace = res.getString("enlace");
                System.out.println(nombre + " -> " + enlace);
            }

            sql = "UPDATE enlaces SET estado = 'revisado' WHERE enlace = '" + enlace + "'";
            preparada = (PreparedStatement) conn.prepareStatement(sql);
            preparada.execute();
        } catch (SQLException ex) {
        } catch (Exception e) {

        }
        return enlace;
    }

    private static void extraerEnlacesURL(Connection conn, String URL) {
        String sitio;

        try {
            URL myURL = new URL(URL);
            sitio = myURL.getHost();
            Document doc = Jsoup.connect(URL).get();
            Elements links = doc.getElementsByTag("a");
            links.stream().map((link) -> link.attr("href")).filter((linkHref) -> (!linkHref.equalsIgnoreCase("") && !linkHref.equalsIgnoreCase("#") && !checaExtension(linkHref))).forEach((linkHref) -> {
                if (fastCmp(linkHref, "http") || fastCmp(linkHref, "www.")) {
                    linkHref = extraerJavascript(linkHref);
                    linkHref = extraerDobleLiga(linkHref);
                    linkHref = extraerDobleLiga(linkHref);
                    insertaEnlace(conn, sitio, linkHref);
                } else {
                    linkHref = extraerJavascript(linkHref);
                    linkHref = extraerDobleLiga(linkHref);
                    linkHref = extraerDobleLiga(linkHref);
                    insertaEnlace(conn, sitio, "http://" + sitio + linkHref);
                }
            });

            //Extraemos la palabra más repetida
            //Obtenemos todo el texto de la página
            String todoTexto = doc.body().text();
            //Removemos las palabras comunes
            todoTexto = remuevePalabrasComunes(todoTexto);

            String[] palabras = todoTexto.split(" ");
            String palabra = "";
            int rep = 0;
            int ind;
            int cnt;
            for (String cad : palabras) {
                if (!cad.equalsIgnoreCase(" ") && !cad.equalsIgnoreCase("|") && cad.length() > 3) {
                    ind = 0;
                    cnt = 0;
                    while (true) {
                        int pos = todoTexto.indexOf(cad, ind);
                        if (pos < 0) {
                            break;
                        }
                        cnt++;
                        ind = pos + 1; // Advance by second.length() to avoid self repetitions
                    }
                    if (cnt > rep) {
                        palabra = cad;
                        rep = cnt;
                    }
                }
            }

            System.out.println(palabra + " : " + rep);
            insertarPalabra(sitio, palabra, URL, rep, conn);
        } catch (IOException e) {
        } catch (Exception e) {

        }
    }

    private static void insertaEnlace(Connection conn, String sitio, String enlace) {
        try {

            //La sentencia mysql
            String sql = "INSERT INTO enlaces (sitio, enlace, estado) VALUES(?, ?, ?)";

            //Crea la sentencia preparada
            preparada = (PreparedStatement) conn.prepareStatement(sql);
            preparada.setString(1, sitio);
            preparada.setString(2, enlace);
            preparada.setString(3, "pendiente");

            //ejecutamos la sentencia
            preparada.execute();

        } catch (SQLException ex) {
        } catch (Exception e) {

        }
    }

    static void insertarPalabra(String sitio, String palabra, String enlace, int repeticiones, Connection conn) {
        try {
            //La sentencia mysql
            String sql = "INSERT INTO palabras (sitio, enlace, palabra, repeticiones) VALUES(?, ?, ?, ?)";
            //Crea la sentencia preparada
            preparada = (PreparedStatement) conn.prepareStatement(sql);
            preparada.setString(1, sitio);
            preparada.setString(2, enlace);
            preparada.setString(3, palabra);
            preparada.setInt(4, repeticiones);
            //ejecutamos la sentencia
            preparada.execute();
        } catch (SQLException ex) {
        } catch (Exception e) {

        }
    }

    static Connection getConexion() {
        Connection conn;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Properties p = new Properties();
            p.put("user", "root");
            p.put("password", "");
            conn = (Connection) DriverManager.getConnection("jdbc:mysql://localhost:3306/perro?allowMultiQueries=true", p);
        } catch (SQLException | ClassNotFoundException ex) {
            return null;
        }
        //e.printStackTrace();
        return conn;
    }

    static String remuevePalabrasComunes(String cadena) {
        String regex;
        //Removemos la palabra "para"
        regex = "\\s*\\bpara\\b\\s*";
        cadena = cadena.replaceAll(regex, "");
        //Removemos la palabra "esta"
        regex = "\\s*\\besta\\b\\s*";
        cadena = cadena.replaceAll(regex, "");
        //Removemos la palabra "quien"
        regex = "\\s*\\bquien\\b\\s*";
        cadena = cadena.replaceAll(regex, "");
        //Removemos la palabra "como"
        regex = "\\s*\\bcomo\\b\\s*";
        cadena = cadena.replaceAll(regex, "");
        //Removemos la palabra "cual"
        regex = "\\s*\\bcual\\b\\s*";
        cadena = cadena.replaceAll(regex, "");
        //Removemos la palabra "quiera"
        regex = "\\s*\\bquiera\\b\\s*";
        cadena = cadena.replaceAll(regex, "");
        //Removemos la palabra "cada"
        regex = "\\s*\\bcada\\b\\s*";
        cadena = cadena.replaceAll(regex, "");
        //Removemos la palabra "dijo"
        regex = "\\s*\\bdijo\\b\\s*";
        cadena = cadena.replaceAll(regex, "");
        //Removemos la palabra "Responder"
        regex = "\\s*\\bResponder\\b\\s*";
        cadena = cadena.replaceAll(regex, "");
        return cadena;
    }

    static boolean fastCmp(String s1, String s2) {
        return s1.regionMatches(0, s2, 0, 4);
    }

    static String extraerJavascript(String link) {
        int index = link.indexOf("javascript:");
        if (index > -1) {
            link = link.substring(0, index);
        }
        return link;
    }

    static String extraerDobleLiga(String link) {
        int index;
        if (link.contains("http:")) {
            index = link.indexOf("http:", link.indexOf("http:") + 1);
            if (index > -1) {
                link = link.substring(0, index);
                return link;
            }
            index = link.indexOf("https:", link.indexOf("http:") + 1);
            if (index > -1) {
                link = link.substring(0, index);
                return link;
            }
        } else if (link.contains("https:")) {
            index = link.indexOf("http:", link.indexOf("https:") + 1);
            if (index > -1) {
                link = link.substring(0, index);
                return link;
            }
            index = link.indexOf("https:", link.indexOf("https:") + 1);
            if (index > -1) {
                link = link.substring(0, index);
                return link;
            }
        }
        return link;
    }

    static boolean checaExtension(String link) {
        String[] types = {"png", "jpg", "gif", "pdf"};
        return Arrays.asList(types).contains(link.substring(link.lastIndexOf('.') + 1));
    }

    private final SecureRandom random = new SecureRandom();

    public String nextSessionId() {
        return new BigInteger(130, random).toString(32);
    }

}
