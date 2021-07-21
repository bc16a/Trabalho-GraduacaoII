//https://www.youtube.com/watch?v=ih1ce855S_A
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet; 
import java.sql.ResultSetMetaData; 
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.jdbc.CalciteMetaImpl; 
import org.apache.calcite.adapter.csv.CsvSchemaFactory; 
import org.apache.calcite.adapter.csv.CsvStreamTableFactory; 
import org.apache.calcite.adapter.csv.CsvStreamScannableTable;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.adapter.java.ReflectiveSchema; 
import org.apache.commons.dbcp2.BasicDataSource; 
import org.apache.calcite.adapter.jdbc.JdbcSchema; 
import org.apache.calcite.adapter.enumerable.EnumerableLimitSortRule;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;

import org.apache.calcite.linq4j.Enumerator;

import java.util.Properties;  
import java.sql.PreparedStatement; 

import java.util.ArrayList;
import java.util.List;


import org.codehaus.commons.compiler.CompileException;

import com.google.common.collect.ImmutableMap;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.hamcrest.Description;
import org.junit.Assert;

import org.apache.calcite.util.Sources;
//import org.junit.jupiter.api.Test;

public class Calcilte {

  public static void main(String[] args)throws ClassNotFoundException {  
   getConexaoCalcite(); 
    
     //testPrepared();
   }

//Método Construtor da Classe// 
public Calcilte(){
        
}

public static class TEST {
 
  public final int ID=0;
  public final String NOME="";
}

public static class DEPTS {

  public final int DEPTNO=0;
  public final String NAME="";
}

public static class HrSchema {
  public final List<TEST> depts = new ArrayList<>();
}

//Método de Conexão//

public static Connection getConexaoCalcite() {
        Connection connection = null;          //atributo do tipo Connection 
  
       try {
           Class.forName("org.apache.calcite.jdbc.Driver"); 
      
            Properties info = new Properties();
           
           info.setProperty("lex", "JAVA");
           info.setProperty("caseSensitive", "true");
 
            
            final String model = "inline:"
            + "{"
            + "  version: '1.0',"
            + "   schemas: ["
            + "     {"
            + "       type: 'custom',"
            + "       name: 'SALES',"
            + "       factory: 'org.apache.calcite.adapter.csv.CsvSchemaFactory',"
            + "       operand: {"
            + "         directory: '/home/bibin/Desktop/TG2/sales'," 
            + "         flavor: 'scannable'" 
            + "       }"
            + "     }"
            + "   ]"
            + "}";
            
            info.setProperty("model", model);
            
             connection = DriverManager.getConnection("jdbc:calcite:", info);
               
             //final String sql = "select * from SALES.CAR";  
             //final PreparedStatement statement = connection.prepareStatement(sql); 
             //final ResultSet resultSet = statement.executeQuery(); 
             //Statement statement = connection.createStatement();
             // ResultSet resultSet = statement.executeQuery(sql);
         
            ResultSet resultSet =  connection.getMetaData().getTables(null, null, null, null);
            
            ResultSet res  =connection.getMetaData().getColumns(null, null, "CAR", "NOME");
            res.next();
            System.out.print(res.getString(3) + " "+java.sql.Types.VARCHAR);
            
             //resultSet.next();
            System.out.println();
            output(resultSet);

            resultSet.close();
            //statement.close();
            connection.close();
       
            System.out.println("deu boa sim deu boa");
            return connection;
        } catch (Exception e) {
          System.out.println("Something went wrong.  deu ruim "+ e.getMessage());
          return null;
        }
      
    }
    
    //
    private static void output(ResultSet resultSet) throws SQLException { 
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        
        int j=1;
        while( j<=columnCount){
        String columnName = metaData.getColumnName(j);
        System.out.print(columnName+" ");
        j++;
        }
        
         System.out.println("");
                
        
          
        
        while (resultSet.next()) {
          for (int i = 1;; i++) {
          
            System.out.print(resultSet.getString(i));
            if (i < columnCount) {
              System.out.print(", ");
            } else {
              System.out.println();
              break;
            }
          }
        }
    }
  
  
  
  static void testPrepared(){
    final Properties properties = new Properties();
    properties.setProperty("caseSensitive", "true");
    try (Connection connection =
        DriverManager.getConnection("jdbc:calcite:", properties)) {
      final CalciteConnection calciteConnection = connection.unwrap(
          CalciteConnection.class);

      final Schema schema =
          CsvSchemaFactory.INSTANCE
              .create(calciteConnection.getRootSchema(), null,
                  ImmutableMap.of("directory",
                      resourcePath("sales"), "flavor", "scannable"));
      calciteConnection.getRootSchema().add("TEST", schema);
      
      
      final String sql = "select * from \"TEST\".\"DEPTS\" where \"NAME\" = ?";
      final PreparedStatement statement2 =
          calciteConnection.prepareStatement(sql);

      statement2.setString(1, "Sales");
      final ResultSet resultSet1 = statement2.executeQuery();
   
    }catch (Exception e) {
          System.out.println("Something went wrong.  deu ruim "+ e.getMessage());
    
        }
  }

  private static String resourcePath(String path) {
    return Sources.of(Calcilte.class.getResource("/" + path)).file().getAbsolutePath();
  }

 
}
/*
javac -classpath .:calcite-core-1.27.0.jar:calcite-csv-1.27.0.jar:commons-dbcp2-2.4.0.jar:calcite-linq4j-1.27.0.jar:mysql-connector-java-8.0.25.jar:avatica-core-1.18.0.jar:protobuf-java-3.17.3.jar:guava-19.0.jar:commons-pool2-2.6.1.jar:commons-logging-1.1.3.jar:slf4j-api-1.7.2.jar:slf4j-simple-1.7.2.jar:json-path-2.2.0.jar:jackson-core-2.10.0.jar:esri-geometry-api-2.2.0.jar:failureaccess-1.0.1.jar:jackson-databind-2.10.0.jar:jackson-dataformat-yaml-2.10.0.jar:jackson-annotations-2.10.0.jar:calcite-server-1.27.0.jar:junit-4.9.jar:hamcrest-all-1.3.jar:checker-3.15.0.jar:commons-compiler-3.0.8.jar:calcite-example-csv-1.14.0.jar:checker-util-3.15.0.jar:checker-qual-3.15.0.jar:calcite-file-1.27.0.jar:opencsv-2.3.jar:commons-lang3-3.8.jar Calcilte.java -Xlint

java -classpath .:calcite-core-1.27.0.jar:calcite-csv-1.27.0.jar:commons-dbcp2-2.4.0.jar:calcite-linq4j-1.27.0.jar:mysql-connector-java-8.0.25.jar:avatica-core-1.18.0.jar:protobuf-java-3.17.3.jar:guava-19.0.jar:commons-pool2-2.6.1.jar:commons-logging-1.1.3.jar:slf4j-api-1.7.2.jar:slf4j-simple-1.7.2.jar:json-path-2.2.0.jar:jackson-core-2.10.0.jar:esri-geometry-api-2.2.0.jar:failureaccess-1.0.1.jar:jackson-databind-2.10.0.jar:jackson-dataformat-yaml-2.10.0.jar:jackson-annotations-2.10.0.jar:calcite-server-1.27.0.jar:junit-4.9.jar:hamcrest-all-1.3.jar:checker-3.15.0.jar:calcite-example-csv-1.14.0.jar:checker-util-3.15.0.jar:checker-qual-3.15.0.jar:calcite-file-1.27.0.jar:opencsv-2.3.jar:commons-lang3-3.8.jar:commons-compiler-3.1.4.jar:commons-compiler-jdk-3.1.4.jar Calcilte -Xlint
*/


