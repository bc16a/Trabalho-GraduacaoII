//https://www.youtube.com/watch?v=ih1ce855S_A
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;  
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


public class Calcilte {

  public static void main(String[] args)throws ClassNotFoundException {  
    getConexaoCalcite(); 
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
            + "{\n"
            + "  version: '1.0',\n"
            + "   schemas: [\n"
            + "     {\n"
            + "       type: 'custom',\n"
            + "       name: 'SALES',\n"
            + "       factory: 'org.apache.calcite.adapter.csv.CsvSchemaFactory',\n"
            + "       operand: {\n"
            + "         directory: '/home/bibin/Desktop/TG2/sales',\n" 
            + "         flavor: 'scannable'\n" 
            + "       }\n"
            + "     }\n"
            + "   ]\n"
            + "}\n";
            
            info.setProperty("model", model);
            
             connection = DriverManager.getConnection("jdbc:calcite:", info);
               
             final String sql = "select * from \"SALES\".\"DEPTS\""; 
             final PreparedStatement statement = connection.prepareStatement(sql); 
             final ResultSet resultSet = statement.executeQuery();


            resultSet.close();
            statement.close();
            connection.close();
       
            System.out.println("deu boa sim deu boa");
            return connection;
        } catch (Exception e) {
          System.out.println("Something went wrong.  deu ruim "+ e.getMessage());
          return null;
        }
      
    }

 
}
/*
javac -classpath .:calcite-core-1.27.0.jar:calcite-csv-1.27.0.jar:commons-dbcp2-2.4.0.jar:calcite-linq4j-1.27.0.jar:mysql-connector-java-8.0.25.jar:avatica-core-1.18.0.jar:protobuf-java-3.17.3.jar:guava-19.0.jar:commons-pool2-2.6.1.jar:commons-logging-1.1.3.jar:slf4j-api-1.7.2.jar:slf4j-simple-1.7.2.jar:json-path-2.2.0.jar:jackson-core-2.10.0.jar:esri-geometry-api-2.2.0.jar:failureaccess-1.0.1.jar:jackson-databind-2.10.0.jar:jackson-dataformat-yaml-2.10.0.jar:jackson-annotations-2.10.0.jar:calcite-server-1.27.0.jar:junit-4.9.jar:hamcrest-all-1.3.jar:checker-3.15.0.jar:commons-compiler-3.0.8.jar:calcite-example-csv-1.14.0.jar:checker-util-3.15.0.jar:checker-qual-3.15.0.jar:calcite-file-1.27.0.jar:opencsv-2.3.jar:commons-lang3-3.8.jar Calcilte.java -Xlint
*/


