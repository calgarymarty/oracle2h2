package com.truecool.ora2h2;

import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.IOUtils;

public class DdlExtractor
{
  private static String TRANSFOR_PARAM_COMMANDS[] = {
      "{call DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', TRUE)}",
      "{call DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', FALSE)}",
      "{call DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'CONSTRAINTS', TRUE)}",
      "{call DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'CONSTRAINTS_AS_ALTER', TRUE)}",
      "{call DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'REF_CONSTRAINTS', FALSE)}",
      "{call DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', FALSE)}",
      "{call DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'TABLESPACE', FALSE)}"
  };

  /**
   * @param connection
   */
  private void setup( Connection connection)
  {
    Arrays.stream( TRANSFOR_PARAM_COMMANDS ).forEach( query -> {
      try ( CallableStatement statement = connection.prepareCall( query ) )
      {
        statement.execute();
      }
      catch ( Exception e )
      {

      }
    } );
  }

  /**
   * To get the DDL…
   * select dbms_metadata.get_ddl('TABLE','CLAIM','CAPS') from dual;
   *
   * @param connection
   * @param tableName
   * @return
   */
  private String getOracleDdl( Connection connection, String schema, String tableName )
  {
    String ddl = null;
    String query = "select dbms_metadata.get_ddl('TABLE','" + tableName + "','" + schema + "') from dual";

    try ( PreparedStatement statement = connection.prepareStatement( query ) )
    {
      ResultSet result = statement.executeQuery();

      if ( result.next() )
      {
        Clob clob = result.getClob( 1 );
        InputStream clobInput = clob.getAsciiStream();
        StringWriter stringWriter = new StringWriter();
        IOUtils.copy( clobInput, stringWriter, Charset.defaultCharset() );

        // RAW DDL from Oracle
        ddl = stringWriter.toString();

        // Additional fixes to make H2 compatible
        ddl = ddl.replaceAll( "(?i)\"" + schema + "\".", "" );
        ddl = ddl.replaceAll( "(?i)USING INDEX ", "" );
        ddl = ddl.replaceAll( "(?i) ENABLE", "" );
      }
    }
    catch ( Exception e )
    {

    }

    return ddl;
  }

  public String oracle2h2( String url, String user, String password, String schema, List<String> tables) throws Exception
  {
    StringBuffer allDdl = new StringBuffer();

    try ( Connection connection = DriverManager.getConnection( url, user, password ) )
    {
      setup( connection );

      tables.stream().forEach( table -> {
        String ddl = getOracleDdl( connection, schema, table );
        allDdl.append( ddl ).append( "\n\r" );
      } );
    }

    return allDdl.toString();
  }

}
