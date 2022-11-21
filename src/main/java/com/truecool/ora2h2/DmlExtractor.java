package com.truecool.ora2h2;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DmlExtractor
{
  public static String buildInsertStatement( String tableName, Map<String, Object> dataMap )
  {
    boolean isFirst = true;

    String returnValue = new StringBuffer( "INSERT INTO " ).append( tableName ).append( " (" ).toString();
    String columnValues = "";

    Iterator iterator = dataMap.keySet().iterator();
    do
    {
      if ( !iterator.hasNext() )
      {
        break;
      }

      String keyName = (String) iterator.next();
      if ( isValidColumnName( keyName ) )
      {
        returnValue = returnValue + ( isFirst ? keyName : ( ",".concat( keyName ) ) );

        Object objectValue = dataMap.get( keyName );
        String stringValue = null;

        if ( objectValue instanceof Timestamp )
        {
          Timestamp timestamp = (Timestamp) objectValue;
          stringValue = new SimpleDateFormat( "yyyy-MM-dd" ).format( timestamp );
        }
        else
        {
          stringValue = objectValue == null ? "NULL" : objectValue.toString();
          stringValue = stringValue.replace( "'", "" );
        }

        columnValues = columnValues + ( isFirst ?
          ( new StringBuffer( "'" ) ).append( stringValue ).append( "'" ) :
          ( new StringBuffer( ", '" ) ).append( stringValue ).append( "'" ) );

        if ( isFirst )
        {
          isFirst = false;
        }
      }
    }
    while ( true );

    returnValue =
      new StringBuffer( returnValue ).append( ") VALUES (" ).append( columnValues ).append( ")" ).append( ";\r\n" ).toString();

    return returnValue;
  }

  public static boolean isValidColumnName( String name )
  {
    boolean returnValue = true;
    if ( name.length() < 1 )
    {
      returnValue = false;
    }
    else if ( name.indexOf( 13 ) == 0 )
    {
      returnValue = false;
    }
    else if ( name.equals( "INT1" ) )
    {
      returnValue = false;
    }
    else if ( name.equals( "INT2" ) )
    {
      returnValue = false;
    }
    else
    {
      returnValue = !isValidInteger( name );
    }
    return returnValue;
  }

  public static boolean isValidInteger( String string )
  {
    boolean returnValue = true;
    int i;
    try
    {
      i = Integer.parseInt( string );
    }
    catch ( NumberFormatException e )
    {
      returnValue = false;
    }
    return returnValue;
  }

  public List<Map<String, Object>> getData( Connection conn, String sql )
  {
    List dataList = null;

    try ( Statement statement = conn.createStatement() )
    {

      try ( ResultSet resultSet = statement.executeQuery( sql ) )
      {
        dataList = new ArrayList();
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int iNumCols = resultSetMetaData.getColumnCount();

        while ( resultSet.next() )
        {
          Map<String, Object> dataMap = new HashMap();
          for ( int index = 1; index <= iNumCols; index++ )
          {
            String columnName = resultSetMetaData.getColumnName( index );
            Object columnValue = resultSet.getObject( index );
            dataMap.put( columnName, columnValue );
          }

          dataList.add( dataMap );
        }
      }
    }
    catch ( SQLException e )
    {
      System.err.println( "SQL Error : " + e.getLocalizedMessage() );
    }

    return dataList;
  }

  public void oracle2h2( String url, String user, String password, List<String> tables, File outFile ) throws Exception
  {
    try ( Connection connection = DriverManager.getConnection( url, user, password ) )
    {
      try ( OutputStream out = new FileOutputStream( outFile ) )
      {
        for ( String table : tables )
        {

          List<Map<String, Object>> data = getData( connection, "SELECT * FROM " + table );

          if ( data != null )
          {
            {
              for ( Map<String, Object> row : data )
              {
                String insert = buildInsertStatement( table, row );
                out.write( insert.getBytes() );
              }
            }
          }

        }
      }
    }
  }
}
